
import io.reactivex.Flowable
import io.reactivex.Observable
import io.reactivex.observers.TestObserver
import io.reactivex.subscribers.TestSubscriber
import junit.framework.Assert.assertFalse
import junit.framework.Assert.assertTrue
import org.junit.Assert
import org.junit.Test
import org.nield.rxkotlinjdbc.*
import java.sql.DriverManager

class DatabaseTest {

    val dbPath = "jdbc:sqlite::memory:"

    val connectionFactory = {

        DriverManager.getConnection(dbPath).apply {
            createStatement().apply {
                execute("CREATE TABLE USER (ID INTEGER PRIMARY KEY, USERNAME VARCHAR(30) NOT NULL, PASSWORD VARCHAR(30) NOT NULL)")
                execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES ('thomasnield','password123')")
                execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES ('bobmarshal','batman43')")
                close()
            }
        }
    }

    @Test
    fun testConnection() {

        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Pair<Int,String>>()

        conn.select("SELECT * FROM USER")
                .toFlowable { it.getInt("ID") to it.getString("USERNAME") }
                .subscribe(testSubscriber)

        testSubscriber.assertValueCount(2)

        conn.close()
    }

    @Test
    fun multiInjectParameterTest() {

        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Pair<Int,String>>()

        conn.select("SELECT * FROM USER WHERE USERNAME LIKE :pattern and PASSWORD LIKE :pattern")
                .parameter("pattern","%b%")
                .toFlowable { it.getInt("ID") to it.getString("USERNAME") }
                .subscribe(testSubscriber)

        testSubscriber.assertValue(Pair(2,"bobmarshal"))

        conn.close()
    }

    @Test
    fun testSequence() {
        val conn = connectionFactory()

        data class User(val id: Int, val userName: String, val password: String)

        conn.select("SELECT * FROM USER").toSequence {
            User(it.getInt("ID"), it.getString("USERNAME"), it.getString("PASSWORD"))
        }.count().let { Assert.assertTrue(it == 2) }
    }
    @Test
    fun testSequenceCancel() {
        val conn = connectionFactory()

        conn.select("SELECT * FROM USER").toSequence { it.getInt("ID") }.apply {
                take(1).forEach{ }

            assertFalse(isClosed)
            close()
            assertTrue(isClosed)
        }
    }

    @Test
    fun testFirst() {
        val conn = connectionFactory()

        val id = conn.select("SELECT * FROM USER WHERE ID = 1").blockingFirst { it.getInt("ID") }

        assertTrue(id == 1)
    }

    @Test
    fun parameterTest() {
        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Pair<Int,String>>()

        conn.select("SELECT * FROM USER WHERE ID = ?")
                .parameter(2)
                .toSingle { it.getInt("ID") to it.getString("USERNAME") }
                .toFlowable()
                .subscribe(testSubscriber)

        testSubscriber.assertValue(Pair(2,"bobmarshal"))

        conn.close()
    }

    @Test
    fun namedParameterTest() {

        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Pair<Int,String>>()

        conn.select("SELECT * FROM USER WHERE ID = :id")
                .parameter("id",2)
                .toSingle { it.getInt("ID") to it.getString("USERNAME") }
                .toFlowable()
                .subscribe(testSubscriber)

        testSubscriber.assertValue(Pair(2,"bobmarshal"))

        conn.close()
    }


    @Test
    fun namedMultipleParameterTest() {

        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Pair<Int,String>>()

        conn.select("SELECT * FROM USER WHERE USERNAME LIKE :pattern and PASSWORD LIKE :pattern")
                .parameter("pattern","%b%")
                .toFlowable { it.getInt("ID") to it.getString("USERNAME") }
                .subscribe(testSubscriber)

        testSubscriber.assertValue(Pair(2,"bobmarshal"))

        conn.close()
    }

    @Test
    fun flatMapSelectTest() {
        val conn = connectionFactory()

        val testObserver = TestObserver<Pair<Int,String>>()

        Observable.just(1,2)
                .flatMapSingle {
                    conn.select("SELECT * FROM USER WHERE ID = :id")
                            .parameter("id",it)
                            .toSingle { it.getInt("ID") to it.getString("USERNAME") }
                }
                .subscribe(testObserver)

        testObserver.assertValueCount(2)

        conn.close()
    }

    @Test
    fun singleInsertTest() {
        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Int>()

        conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
                .parameter("username","josephmarlon")
                .parameter("password","coffeesnob43")
                .toFlowable { it.getInt(1) }
                .flatMapSingle {
                    conn.select("SELECT * FROM USER WHERE ID = :id")
                            .parameter("id", it)
                            .toSingle { it.getInt("id") }
                }
                .subscribe(testSubscriber)

        testSubscriber.assertValues(3)

        conn.close()
    }


    @Test
    fun singleInsertTest2() {
        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Int>()

        conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
                .parameter("username" to "josephmarlon")
                .parameter("password" to "coffeesnob43")
                .toFlowable { it.getInt(1) }
                .flatMapSingle {
                    conn.select("SELECT * FROM USER WHERE ID = :id")
                            .parameter("id", it)
                            .toSingle { it.getInt("id") }
                }
                .subscribe(testSubscriber)

        testSubscriber.assertValues(3)

        conn.close()
    }

    @Test
    fun singleInsertTest3() {
        val conn = connectionFactory()

        val testSubscriber = TestSubscriber<Int>()

        conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (?,?)")
                .parameters("josephmarlon","coffeesnob43")
                .toFlowable { it.getInt(1) }
                .flatMapSingle {
                    conn.select("SELECT * FROM USER WHERE ID = :id")
                            .parameter("id", it)
                            .toSingle { it.getInt("id") }
                }
                .subscribe(testSubscriber)

        testSubscriber.assertValues(3)

        conn.close()
    }


    @Test
    fun blockingInsertTest() {
        val conn = connectionFactory()

        conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
                .parameter("username","josephmarlon")
                .parameter("password","coffeesnob43")
                .blockingFirst {
                    val id = it.getInt(1)
                    conn.select("SELECT * FROM USER WHERE ID = :id")
                            .parameter("id", id)
                            .blockingFirst { it.getInt("id") }
                }.apply {
                    assertTrue(this > 0)
                }


        conn.close()
    }

    @Test
    fun multiInsertTest() {
        val conn = connectionFactory()
        val testObserver = TestObserver<Int>()

        Observable.just(
                Pair("josephmarlon", "coffeesnob43"),
                Pair("samuelfoley","shiner67"),
                Pair("emilyearly","rabbit99")
        ).flatMapSingle {
            conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)")
                    .parameter("username",it.first)
                    .parameter("password",it.second)
                    .toSingle { it.getInt(1) }
        }.subscribe(testObserver)

        testObserver.assertValues(3,4,5)

        conn.close()
    }

    @Test
    fun multiInsertTest2() {
        val conn = connectionFactory()
        val testObserver = TestObserver<Int>()

        Observable.just(
                Pair("josephmarlon", "coffeesnob43"),
                Pair("samuelfoley","shiner67"),
                Pair("emilyearly","rabbit99")
        ).flatMapSingle {
            conn.insert("INSERT INTO USER (USERNAME, PASSWORD) VALUES (?,?)")
                    .parameters(it.first, it.second)
                    .toSingle { it.getInt(1) }
        }.subscribe(testObserver)

        testObserver.assertValues(3,4,5)

        conn.close()
    }


    @Test
    fun deleteTest() {

        val conn = connectionFactory()

        val testObserver = TestObserver<Int>()

        conn.execute("DELETE FROM USER WHERE ID = :id")
                .parameter("id",2)
                .toSingle()
                .subscribe(testObserver)

        testObserver.assertValues(1)
        conn.close()
    }
    @Test
    fun updateTest() {

        val conn = connectionFactory()
        val testObserver = TestObserver<Int>()

        conn.execute("UPDATE USER SET PASSWORD = :password WHERE ID = :id")
                .parameter("id",1)
                .parameter("password","squirrel56")
                .toSingle()
                .subscribe(testObserver)

        testObserver.assertValues(1)

        conn.close()
    }
    @Test
    fun updateTest2() {

        val conn = connectionFactory()
        val testObserver = TestObserver<Int>()

        conn.execute("UPDATE USER SET PASSWORD = ? WHERE ID = ?")
                .parameters("squirrel56", 1)
                .toSingle()
                .subscribe(testObserver)

        testObserver.assertValues(1)

        conn.close()
    }

    @Test
    fun asListTest() {
        val conn = connectionFactory()
        val testObserver = TestObserver<List<Any?>>()


        conn.select("SELECT * FROM USER WHERE ID = ?")
                .parameter(1)
                .toObservable { it.asList() }
                .subscribe(testObserver)

        testObserver.assertValues(listOf(1, "thomasnield", "password123"))
    }

    @Test
    fun asMapTest() {
        val conn = connectionFactory()
        val testObserver = TestObserver<Map<String,Any?>>()


        conn.select("SELECT * FROM USER WHERE ID = ?")
                .parameter(1)
                .toObservable { it.asMap() }
                .subscribe(testObserver)

        testObserver.assertValues(mapOf("ID" to 1, "USERNAME" to "thomasnield", "PASSWORD" to "password123"))
    }

    @Test
    fun pipelineTest() {
        val conn = connectionFactory()

        data class User(val userName: String, val password: String)

        val testObserver = TestObserver<User>()
        val testSubscriber = TestSubscriber<User>()

        val pipeline = conn.select("SELECT * FROM USER")
                .toPipeline {
                    User(it.getString("USERNAME"), it.getString("PASSWORD"))
                }


        pipeline.toObservable().subscribe(testObserver)

        testObserver.assertValues(
                User("thomasnield","password123"),
                User("bobmarshal","batman43")
        )


        pipeline.toFlowable().subscribe(testSubscriber)

        testSubscriber.assertValues(
                User("thomasnield","password123"),
                User("bobmarshal","batman43")
        )

        Assert.assertTrue(
            pipeline.toSequence().toSet() == setOf(
                    User("thomasnield","password123"),
                    User("bobmarshal","batman43")
            )
        )
    }

    @Test
    fun whereOptional1() {
        val conn = connectionFactory()

        fun userOf(id: Int? = null, userName: String? = null, password: String? = null) =
                conn.select("SELECT * FROM USER")
                        .whereOptional("ID", id)
                        .whereOptional("USERNAME", userName)
                        .whereOptional("PASSWORD", password)
                        .toObservable { it.getInt("ID") }


        val observer1 = TestObserver<Int>()
        userOf().subscribe(observer1)
        observer1.assertValues(1,2)

        val observer2 = TestObserver<Int>()
        userOf(userName = "thomasnield", password = "password123").subscribe(observer2)
        observer2.assertValues(1)

        val observer3 = TestObserver<Int>()
        userOf(id = 2).subscribe(observer3)
        observer3.assertValues(2)
    }


    @Test
    fun whereOptional2() {
        val conn = connectionFactory()

        fun userOf(id: Int? = null, userName: String? = null, password: String? = null) =
                conn.select("SELECT * FROM USER")
                        .whereOptional("ID = :id", id)
                        .whereOptional("USERNAME = :username", userName)
                        .whereOptional("PASSWORD = :password", password)
                        .toObservable { it.getInt("ID") }


        val observer2 = TestObserver<Int>()
        userOf(userName = "thomasnield", password = "password123").subscribe(observer2)
        observer2.assertValues(1)
    }


    @Test
    fun batchInsertObservable() {

        val conn = connectionFactory()

        val insertElements = Observable.just(
                Pair("josephmarlon", "coffeesnob43"),
                Pair("samuelfoley","shiner67"),
                Pair("emilyearly","rabbit99"),
                Pair("johnlawrey", "shiner23"),
                Pair("tomstorm","coors44"),
                Pair("danpaxy", "texas22"),
                Pair("heathermorgan","squirrel22")
        )

        conn.batchExecute(
                sqlTemplate = "INSERT INTO USER (USERNAME, PASSWORD) VALUES (?,?)",
                elements = insertElements,
                batchSize = 3,
                parameterMapper = { parameters(it.first, it.second) },
                autoClose = false
        ).toObservable()
         .subscribe()

        val testObserver = TestObserver<Pair<String,String>>()

        conn.select("SELECT * FROM USER")
                .toObservable { it.getString(1) to it.getString(2) }
                .subscribe(testObserver)

        testObserver.assertValueCount(9)
    }

    @Test
    fun batchInsertFlowable() {

        val conn = connectionFactory()

        val insertElements = Flowable.just(
                Pair("josephmarlon", "coffeesnob43"),
                Pair("samuelfoley","shiner67"),
                Pair("emilyearly","rabbit99"),
                Pair("johnlawrey", "shiner23"),
                Pair("tomstorm","coors44"),
                Pair("danpaxy", "texas22"),
                Pair("heathermorgan","squirrel22")
        )

        conn.batchExecute(
                sqlTemplate = "INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)",
                elements = insertElements,
                batchSize = 3,
                parameterMapper = {
                    parameter("username", it.first)
                    parameter("password", it.second)
                }
        ).toFlowable()
         .subscribe()

        val testSubscriber = TestSubscriber<Pair<String,String>>()

        conn.select("SELECT * FROM USER")
                .toFlowable { it.getString(1) to it.getString(2) }
                .subscribe(testSubscriber)

        testSubscriber.assertValueCount(9)
    }

    @Test
    fun batchInsertSequence() {

        val conn = connectionFactory()

        val insertElements = listOf(
                Pair("josephmarlon", "coffeesnob43"),
                Pair("samuelfoley","shiner67"),
                Pair("emilyearly","rabbit99"),
                Pair("johnlawrey", "shiner23"),
                Pair("tomstorm","coors44"),
                Pair("danpaxy", "texas22"),
                Pair("heathermorgan","squirrel22")
        )

        conn.batchExecute(
                sqlTemplate = "INSERT INTO USER (USERNAME, PASSWORD) VALUES (:username,:password)",
                elements = insertElements,
                batchSize = 3,
                parameterMapper = {
                    parameter("username", it.first)
                    parameter("password", it.second)
                }
        ).toSequence().count()

        conn.select("SELECT * FROM USER")
                .toSequence { it.getString(1) to it.getString(2) }
                .toList()
                .also { Assert.assertTrue(it.count() == 9) }
    }
}