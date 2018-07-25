# RxKotlin-JDBC

Fluent, concise, and easy-to-use extension functions targeting JDBC in the Kotlin language with [RxJava 2.0](https://github.com/ReactiveX/RxJava).

This library is inspired by Dave Moten's [RxJava-JDBC](https://github.com/davidmoten/rxjava-jdbc) but seeks to be much more lightweight by leveraging Kotlin functions. This works with threadpool `DataSource` implementations such as [HikariCP](https://github.com/brettwooldridge/HikariCP), but can also be used with vanilla JDBC `Connection`s.

Extension functions like `select()`, `insert()`, and `execute()` will target both `DataSource` and JDBC `Connection` types. There is also support for **safe** blocking `Sequence` operations.

## Binaries

**Maven**

```xml
<dependency>
  <groupId>org.nield</groupId>
  <artifactId>rxkotlin-jdbc</artifactId>
  <version>0.3.2</version>
</dependency>
```

**Gradle**

```groovy
repositories {
    mavenCentral()
}
dependencies {
    compile 'org.nield:rxkotlin-jdbc:0.3.2'
}
```


## Managing JDBC with Observables and Flowables

### Using DataSources

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

### Using Connections

You can also use a standard `Connection` with these extension functions, and closing will not happen automatically so you can micromanage the life of that connection. It is also helpful to execute transactions against an individual connection. 

```kotlin
val connection = DriverManager.getConnection("jdbc:sqlite::memory:")

connection.select("SELECT * FROM USER")
        .toObservable { it.getInt("ID") to it.getString("USERNAME") }
        .subscribe(::println)
        
connection.close()
```


## Blocking Sequences

It can be convenient to work with JDBC fluently in a blocking manner, so you don't always have to return everything as an `Observable` or `Flowable`. While you should strive to not break the monad, sometimes it is easier to not have items returned in reactive fashion. This is especially the case when you have reactively built `T` objects, but you want to retrieve metadata `U` and `V` objects when `T` is constructed.

You could try to use RxJava's native blocking operators, but these can have undesired side effects of interruption exceptions especially when the blocking operation is happening under a parent reactive operation that can be disposed. Therefore, these `Sequence` parts of the API do not rely on RxJava at all and are implemented at the `Iterable` level. These may be candidate to be moved to a separate library (and be used as a dependency in this one) before rxkotlin-jdbc reaches a 1.0 release. This transition is not planned to be very breaking, other than perhaps a few import changes. 

For instance, we can call `toSequence()` to iterate a `ResultSet` as a `Sequence`.

```kotlin
data class User(val id: Int, val userName: String, val password: String)

conn.select("SELECT * FROM USER").toSequence {
    User(it.getInt("ID"), it.getString("USERNAME"), it.getString("PASSWORD"))
}.forEach(::println)
```

The `toSequence()` actually returns a `ResultSetSequence`, which is important because if you use Sequence operators like `take()`, the iteration will need to manually be terminated early (completion events are not communicated like in RxJava). The returned `ResultSetSequence` should provide a handle to do this. 

```kotlin 
val conn = connectionFactory()

conn.select("SELECT * FROM USER").toSequence { it.getInt("ID") }.apply {

        take(1).forEach(::println)
        close() // must manually close for operators that terminate iteration early
}
```

Typically, if you simply want to return one item from a `ResultSet`, just call `blockingFirst()` or `blockingFirstOrNull()` which will handle the `close()` operation for you. 

```kotlin
val conn = connectionFactory()

val id = conn.select("SELECT * FROM USER WHERE ID = 1").blockingFirst { it.getInt("ID") }

println(id)
```

## Allowing Choice of Sequence, Observable, or Flowable

This library supports emitting `T` items built off a `ResultSet` in the form of:

* `Sequence<T>`
* `Observable<T>`
* `Flowable<T>`
* `Single<T>`
* `Maybe<T>`

If you are building an API, it may be handy to allow the user of the API to choose the means in which to receive the results.

The `toPipeline()` function will allow mapping the `ResultSet` to `T` items, but defer to the API user how to receive the results.


```kotlin

data class User(val userName: String, val password: String)

fun getUsers() = conn.select("SELECT * FROM USER")
                .toPipeline {
                    User(it.getString("USERNAME"), it.getString("PASSWORD"))
                }

fun main(args: Array<String>) {

    getUsers().toFlowable().subscribe { println("Receiving $it via Flowable") }

    getUsers().toObservable().subscribe { println("Receiving $it via Observable") }

    getUsers().toSequence().forEach { println("Receiving $it via Sequence") }
}
```

## Building Where Conditions Fluently

An experimental feature in RxKotlin-JDBC is an API-friendly builder for WHERE conditions, that may or may not use certain parameters to build WHERE conditions.

For instance, here is a function that will query a table with three possible parameters:

```kotlin
fun userOf(id: Int? = null, userName: String? = null, password: String? = null) =
        conn.select("SELECT * FROM USER")
                .whereOptional("ID", id)
                .whereOptional("USERNAME", userName)
                .whereOptional("PASSWORD", password)
                .toObservable(::User)
```

We can then query for any combination of these three parameters (including none of them).

```kotlin
userOf().subscribe { println(it) } // prints all users

userOf(userName = "thomasnield", password = "password123")
     .subscribe { println(it) }  // prints user with ID 1


userOf(id = 2).subscribe { println(it) } // prints user with ID 2
```

Instead of providing a simple field name, we can also provide an entire templated expression for more complex condtions.

```kotlin
conn.select("SELECT * FROM USER")
       .whereOptional("ID > ?", 1)
```
