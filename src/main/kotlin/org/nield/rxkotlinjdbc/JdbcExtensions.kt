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
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource

fun Connection.execute(sql: String, vararg v: Any?) = Single.fromCallable {
    prepareStatement(sql).let {
        it.processParameters(v)
        it.executeUpdate()
        it.updateCount
    }
}

fun Connection.select(sql: String, vararg v: Any?)  =
        StagedOperation(
            sqlTemplate = sql,
            connectionGetter = { this },
            preparedStatementGetter =  {
                val ps = prepareStatement(sql)
                ps.processParameters(v)
                ps
            },
            resultSetGetter = { it.executeQuery() },
            autoClose = false
    )


fun Connection.insert(insertSQL: String, vararg v: Any?)  =
        StagedOperation(
            sqlTemplate = insertSQL,
            connectionGetter = { this },
            preparedStatementGetter =  {
                val ps = prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)
                ps.processParameters(v)
                ps.executeUpdate()
                ps
            },
            resultSetGetter = { it.generatedKeys },
            autoClose = false
    )

fun DataSource.execute(sql: String, vararg v: Any?): Int {
    val c = connection
    val ps = connection.prepareStatement(sql)
    ps.processParameters(v)
    val affectedCount = ps.executeUpdate()
    ps.close()
    c.close()
    return affectedCount
}

fun DataSource.select(sql: String, vararg v: Any?)  =
        StagedOperation(
            sqlTemplate = sql,
            connectionGetter = { this.connection },
            preparedStatementGetter =  {
                val ps = connection.prepareStatement(sql)
                ps.processParameters(v)
                ps
            },
            resultSetGetter = { it.executeQuery() },
            autoClose = true
    )



fun DataSource.insert(insertSQL: String, vararg v: Any?) =
        StagedOperation(
            sqlTemplate = insertSQL,
            connectionGetter = { this.connection },
            preparedStatementGetter =  {
                val ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)
                ps.processParameters(v)
                ps.executeUpdate()
                ps
            },
            resultSetGetter = { it.generatedKeys },
            autoClose = true
    )



class StagedOperation(
        val sqlTemplate: String,
        val connectionGetter: () -> Connection,
        val preparedStatementGetter: (Connection) -> PreparedStatement,
        val resultSetGetter: ((PreparedStatement) -> ResultSet),
        val autoClose: Boolean,
        val parameters: MutableMap<Int,String> = mutableMapOf()
) {

    fun <T: Any> toObservable(mapper: (ResultSet) -> T) = Observable.defer {
        val conn = connectionGetter()
        val ps = preparedStatementGetter(conn)

        ResultSetState({resultSetGetter(ps)}, ps, conn, autoClose).toObservable(mapper)
    }

    fun <T: Any> toFlowable(mapper: (ResultSet) -> T) = Flowable.defer {
        val conn = connectionGetter()
        val ps = preparedStatementGetter(conn)

        ResultSetState({resultSetGetter(ps)}, ps, conn, autoClose).toFlowable(mapper)
    }

    fun <T: Any> toSingle(mapper: (ResultSet) -> T) = Single.defer {
        toObservable(mapper).singleOrError()
    }

    fun <T: Any> toMaybe(mapper: (ResultSet) -> T) = Maybe.defer {
        toObservable(mapper).singleElement()
    }

    fun <T: Any> toSequence(mapper: (ResultSet) -> T) =
            toObservable(mapper).blockingIterable().asSequence()

}

class ResultSetState(
        val resultSetGetter: () -> ResultSet,
        val statement: PreparedStatement? = null,
        val connection: Connection? = null,
        val autoClose: Boolean
) {
    fun <T: Any> toObservable(mapper: (ResultSet) -> T): Observable<T> {

        return Observable.defer {
            val iterator = QueryIterator(this, resultSetGetter(), mapper, autoClose)
            Observable.fromIterable(iterator.asIterable())
                    .doOnTerminate { iterator.close() }
                    .doOnDispose { iterator.close() }
        }
    }

    fun <T: Any> toFlowable(mapper: (ResultSet) -> T): Flowable<T> {
        return Flowable.defer {
            val iterator = QueryIterator(this, resultSetGetter(), mapper, autoClose)
            Flowable.fromIterable(iterator.asIterable())
                    .doOnTerminate { iterator.close() }
                    .doOnCancel { iterator.cancel() }
        }
    }
}

class QueryIterator<out T>(val qs: ResultSetState,
                           val rs: ResultSet,
                           val mapper: (ResultSet) -> T,
                           val autoClose: Boolean
) : Iterator<T> {

    private var didNext = false
    private var hasNext = false
    private val cancelled = AtomicBoolean(false)

    override fun next(): T {
        if (!didNext) {
            rs.next()
        }
        didNext = false
        return mapper(rs)
    }

    override fun hasNext(): Boolean {
        if (cancelled.get()) {
            excecuteCancel()
            hasNext = false
            return false
        }
        if (!didNext) {
            hasNext = rs.next()
            didNext = true
        }
        return hasNext
    }

    fun asIterable() = object: Iterable<T> {
        override fun iterator(): Iterator<T> = this@QueryIterator
    }

    fun close() {
        rs.close()
        qs.statement?.close()
        if (autoClose)
            qs.connection?.close()
    }
    fun cancel() {
        cancelled.set(true)
    }
    private fun excecuteCancel() {
        rs.close()
        qs.statement?.close()
        if (autoClose)
            qs.connection?.close()
    }
}

fun PreparedStatement.processParameters(v: Array<out Any?>) = v.forEachIndexed { pos, argVal ->
    when (argVal) {
        null -> setObject(pos+1, null)
        is UUID -> setObject(pos+1, argVal)
        is Int -> setInt(pos+1, argVal)
        is String -> setString(pos+1, argVal)
        is Double -> setDouble(pos+1, argVal)
        is Boolean -> setBoolean(pos+1, argVal)
        is Float -> setFloat(pos+1, argVal)
        is Long -> setLong(pos+1, argVal)
        is LocalTime -> setTime(pos+1, java.sql.Time.valueOf(argVal))
        is LocalDate -> setDate(pos+1, java.sql.Date.valueOf(argVal))
        is LocalDateTime -> setTimestamp(pos+1, java.sql.Timestamp.valueOf(argVal))
        is BigDecimal -> setBigDecimal(pos+1, argVal)
        is InputStream -> setBinaryStream(pos+1, argVal)
        is Enum<*> -> setObject(pos+1, argVal)
    }
}
