
import io.reactivex.Observable
import io.reactivex.observers.TestObserver
import io.reactivex.subscribers.TestSubscriber
import org.junit.Test
import org.nield.rxkotlinjdbc.execute
import org.nield.rxkotlinjdbc.insert
import org.nield.rxkotlinjdbc.select
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
                            .toSingle { "${it.getInt("ID")} ${it.getString("USERNAME")} ${it.getString("PASSWORD")}" }
                }
                .subscribe(::println)

        testSubscriber.assertValues(3)

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
    fun deleteTest() {

        val conn = connectionFactory()

        val testObserver = TestObserver<Int>()

        conn.execute("DELETE FROM USER WHERE ID = :id")
                .parameter("id",2)
                .toObservable { it.getInt(1) }
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
                .toObservable { it.getInt(1) }
                .subscribe(testObserver)

        testObserver.assertValues(1)

        conn.close()
    }
}