# RxKotlin-JDBC

Fluent, concise, and easy-to-use extension functions targeting JDBC in the Kotlin language with [RxJava 2.0](https://github.com/ReactiveX/RxJava).

This library is inspired by Dave Moten's [RxJava-JDBC](https://github.com/davidmoten/rxjava-jdbc) but seeks to be much more lightweight by leveraging Kotlin functions. This works with threadpool `DataSource` implementations such as [HikariCP](https://github.com/brettwooldridge/HikariCP), but can also be used with vanilla JDBC `Connection`s.

Extension functions like `select()`, `insert()`, and `execute()` will target both `DataSource` and JDBC `Connection` types.

## Binaries


**Maven**

```xml
<dependency>
  <groupId>org.nield</groupId>
  <artifactId>rxkotlin-jdbc</artifactId>
  <version>0.1.1</version>
</dependency>
```

**Gradle**

```groovy
repositories {
    mavenCentral()
}
dependencies {
    compile 'org.nield:rxkotlin-jdbc:0.1.1'
}
```


You can also use JitPack.io to build a snapshot with Maven or Gradle:

**Gradle**

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}
dependencies {
    compile 'com.github.thomasnield:rxkotlin-jdbc:-SNAPSHOT'
}
```

**Maven**

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.thomasnield</groupId>
    <artifactId>rxkotlin-jdbc</artifactId>
    <version>-SNAPSHOT</version>
</dependency>
```

## DataSource Usage Examples

When you use a `DataSource`, a `Connection` will automatically be pulled from the pool upon subscription and given back when `onComplete` is called.


```kotlin
val config = HikariConfig()
config.jdbcUrl = "jdbc:sqlite::memory:"
config.minimumIdle = 3
config.maximumPoolSize = 10

val ds = HikariDataSource(config)

//initialize

with(ds) {
    execute("CREATE TABLE USER (ID INTEGER PRIMARY KEY, USERNAME VARCHAR(30) NOT NULL, PASSWORD VARCHAR(30) NOT NULL)")
    execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES (?,?)", "thomasnield", "password123")
    execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES (?,?)", "bobmarshal","batman43")
}

// Retrieve all users
ds.select("SELECT * FROM USER")
        .toObservable { it.getInt("ID") to it.getString("USERNAME") }
        .subscribe(::println)


// Retrieve user with specific ID
ds.select("SELECT * FROM USER WHERE ID = :id")
        .parameter("id", 2)
        .toSingle { it.getInt("ID") to it.getString("USERNAME") }
        .subscribeBy(::println)

// Execute insert which return generated keys, and re-select the inserted record with that key
ds.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
        .parameter("username","josephmarlon")
        .parameter("password","coffeesnob43")
        .toFlowable { it.getInt(1) }
        .flatMapSingle {
            conn.select("SELECT * FROM USER WHERE ID = :id")
                    .parameter("id", it)
                    .toSingle { "${it.getInt("ID")} ${it.getString("USERNAME")} ${it.getString("PASSWORD")}" }
        }
        .subscribe(::println)

// Run deletion

conn.execute("DELETE FROM USER WHERE ID = :id")
        .parameter("id",2)
        .toSingle()
        .subscribeBy(::println)
```

## Connection Usage Example

You can also use a standard `Connection` with these extension functions, and closing will not happen automatically so you can micromanage the life of that connection.

```kotlin
val connection = DriverManager.getConnection("jdbc:sqlite::memory:")

connection.select("SELECT * FROM USER")
        .toObservable { it.getInt("ID") to it.getString("USERNAME") }
        .subscribe(::println)

```