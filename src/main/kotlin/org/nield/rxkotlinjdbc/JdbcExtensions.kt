package org.nield.rxkotlinjdbc
import io.reactivex.Flowable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import javax.sql.DataSource

fun Connection.execute(sql: String, vararg v: Any?) = Single.fromCallable {
    prepareStatement(sql).let {
        it.processParameters(v)
        it.executeUpdate()
        it.updateCount
    }
}

fun Connection.select(sql: String, vararg v: Any?) = {
    prepareStatement(sql).let { ps ->
        ps.processParameters(v)
        QueryState(ps.executeQuery(), ps)
    }
}

fun Connection.insert(insertSQL: String, vararg v: Any?) =  {
    val ps = prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)
    ps.processParameters(v)
    ps.executeUpdate()
    QueryState(ps.generatedKeys, ps)
}

fun DataSource.execute(sql: String, vararg v: Any?): Int {
    val c = connection
    try {
        val ps = connection.prepareStatement(sql)
        ps.processParameters(v)
        return ps.executeUpdate()
    } finally {
        c.close()
    }
}

fun DataSource.select(sql: String, vararg v: Any?) = {
    connection.let { connection ->
        val ps = connection.prepareStatement(sql)
        ps.processParameters(v)
        QueryState(ps.executeQuery(), ps, connection)
    }
}

fun DataSource.insert(insertSQL: String, vararg v: Any?) = {
    val connection = this.connection
    val ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)
    ps.processParameters(v)
    ps.executeUpdate()
    QueryState(ps.generatedKeys, ps)
}

fun <T: Any> (() -> QueryState).toObservable(mapper: (ResultSet) -> T) = Observable.defer {
    this().toObservable(mapper)
}

fun <T: Any> (() -> QueryState).toFlowable(mapper: (ResultSet) -> T) = Flowable.defer {
    this().toFlowable(mapper)
}

fun <T: Any> (() -> QueryState).toSingle(mapper: (ResultSet) -> T) = Single.defer {
    toObservable(mapper).singleOrError()
}

fun <T: Any> (() -> QueryState).toMaybe(mapper: (ResultSet) -> T) = Maybe.defer {
    toObservable(mapper).singleElement()
}

fun <T: Any> (() -> QueryState).toSequence(mapper: (ResultSet) -> T) =
        toObservable(mapper).blockingIterable()
                .asSequence()

class QueryState(
        val resultSet: ResultSet,
        val statement: PreparedStatement? = null,
        val connection: Connection? = null
) {
    fun <T: Any> toObservable(mapper: (ResultSet) -> T): Observable<T> {
        val iterator = QueryIterator(this,mapper)
        return Observable.fromIterable(iterator.asIterable())
                .doOnTerminate { iterator.close() }
                .doOnDispose { iterator.close() }
    }

    fun <T: Any> toFlowable(mapper: (ResultSet) -> T): Flowable<T> {
        val iterator = QueryIterator(this,mapper)
        return Flowable.fromIterable(iterator.asIterable())
                .doOnTerminate { iterator.close() }
                .doOnCancel { iterator.close() }
    }
}

class QueryIterator<out T>(val qs: QueryState,
                           val mapper: (ResultSet) -> T
) : Iterator<T> {

    private var didNext = false
    private var hasNext = false

    override fun next(): T {
        if (!didNext) {
            qs.resultSet.next()
        }
        didNext = false
        return mapper(qs.resultSet)
    }

    override fun hasNext(): Boolean {
        if (!didNext) {
            hasNext = qs.resultSet.next()
            didNext = true
        }
        return hasNext
    }

    fun asIterable() = object: Iterable<T> {
        override fun iterator(): Iterator<T> = this@QueryIterator
    }

    fun close() {
        qs.resultSet.close()
        qs.statement?.close()
        qs.connection?.close()
    }
}


fun PreparedStatement.processParameters(v: Array<out Any?>) = v.forEachIndexed { pos, v ->
    when (v) {
        null -> setObject(pos+1, null)
        is UUID -> setObject(pos+1, v)
        is Int -> setInt(pos+1, v)
        is String -> setString(pos+1, v)
        is Double -> setDouble(pos+1, v)
        is Boolean -> setBoolean(pos+1, v)
        is Float -> setFloat(pos+1, v)
        is Long -> setLong(pos+1, v)
        is LocalTime -> setTime(pos+1, java.sql.Time.valueOf(v))
        is LocalDate -> setDate(pos+1, java.sql.Date.valueOf(v))
        is LocalDateTime -> setTimestamp(pos+1, java.sql.Timestamp.valueOf(v))
        is BigDecimal -> setBigDecimal(pos+1, v)
        is InputStream -> setBinaryStream(pos+1, v)
        is Enum<*> -> setObject(pos+1, v)
    }
}
