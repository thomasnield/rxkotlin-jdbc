
import org.junit.Test
import org.nield.rxkotlinjdbc.select
import java.sql.DriverManager

class DatabaseTest {

    val dbPath = "jdbc:sqlite::memory:"

    @Test
    fun testConnection() {

        val conn = DriverManager.getConnection(dbPath).apply {
            createStatement().apply {
                execute("CREATE TABLE USER (ID INTEGER PRIMARY KEY, USERNAME VARCHAR(30) NOT NULL, PASSWORD VARCHAR(30) NOT NULL)")
                execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES ('thomasnield','password123')")
                execute("INSERT INTO USER (USERNAME,PASSWORD) VALUES ('bobmarshal','batman43')")
                close()
            }
        }

        conn.select("SELECT * FROM USER")
                .toFlowable { it.getInt("ID") to it.getString("USERNAME") }
                .subscribe(::println)

        conn.close()
    }
}