
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
        }.forEach(::println)
    }
    @Test
    fun testSequenceCancel() {
        val conn = connectionFactory()

        conn.select("SELECT * FROM USER").toSequence { it.getInt("ID") }.apply {
                take(1).forEach(::println)

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
                    println(this)
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
}