package org.nield.rxkotlinjdbc

import io.reactivex.Flowable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.annotations.Experimental
import io.reactivex.functions.BiFunction
import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement.RETURN_GENERATED_KEYS
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

fun Connection.execute(sqlTemplate: String) = UpdateOperation(
        sqlTemplate = sqlTemplate,
        connectionGetter = { this },
        autoClose = false
)

fun Connection.select(sqlTemplate: String)  =
        SelectOperation(
            sqlTemplate = sqlTemplate,
            connectionGetter = { this },
            autoClose = false
        )

/**
 * Executes an INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun Connection.insert(insertSQL: String)  =
        InsertOperation(
            sqlTemplate = insertSQL,
            connectionGetter = { this },
            autoClose = false
        )


/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> Connection.batchExecute(sqlTemplate: String,
                                elements: Observable<T>,
                                batchSize: Int,
                                parameterMapper: PreparedStatement.(T) -> Unit,
                                autoClose: Boolean = false
) = BatchExecute(
    sqlTemplate = sqlTemplate,
    elements =  elements,
    batchSize = batchSize,
    parameterMapper = parameterMapper,
    connectionGetter = { this },
    autoClose = autoClose
)


fun DataSource.execute(sqlTemplate: String) = UpdateOperation(
        sqlTemplate = sqlTemplate,
        connectionGetter = { connection },
        autoClose = true
)

fun DataSource.select(sqlTemplate: String)  =
        SelectOperation(
            sqlTemplate = sqlTemplate,
            connectionGetter = { this.connection },
            autoClose = true
        )



fun DataSource.insert(insertSQL: String) =
        InsertOperation(
            sqlTemplate = insertSQL,
            connectionGetter = { this.connection },
            autoClose = true
        )


class PreparedStatementBuilder(
        val connectionGetter: () -> Connection,
        val preparedStatementGetter: (String,Connection) -> PreparedStatement,
        var sqlTemplate: String

) {

    private val namelessParameterIndex = AtomicInteger(-1)
    val sql: String by lazy { sqlTemplate.replace(parameterRegex,"?") }
    val furtherOps: MutableList<(PreparedStatement) -> Unit> = mutableListOf()

    companion object {
        private val parameterRegex = Regex(":[_A-Za-z0-9]+")
    }

    private val mappedParameters by lazy {
        parameterRegex.findAll(sqlTemplate).asSequence()
                .map { it.value }
                .withIndex()
                .groupBy({ it.value }, { it.index })
    }

    fun parameter(value: Any?) {
        furtherOps += { it.parameter(namelessParameterIndex.incrementAndGet(), value) }
    }

    fun parameters(vararg parameters: Any?) {
        if (parameters[0] is Array<*>) {
            (parameters[0] as Array<*>).forEach {
                parameter(it)
            }
        } else {
            parameters.forEach {
                parameter(it)
            }
        }
    }

    fun parameter(parameter: Pair<String,Any?>) {
        parameter(parameter.first, parameter.second)
    }
    fun parameter(parameter: String, value: Any?) {
        (mappedParameters[":$parameter"] ?: throw Exception("Parameter $parameter not found!}"))
                .asSequence()
                .forEach { i -> furtherOps += { it.parameter(i, value) } }
    }

    private var conditionCount = 0

    @Experimental
    fun whereOptional(field: String, value: Any?) {
        if (value != null) {
            if (conditionCount == 0) {
                sqlTemplate = "$sqlTemplate WHERE"
            }
            conditionCount++

            if (conditionCount > 1) {
                sqlTemplate = "$sqlTemplate AND "
            }

            sqlTemplate = if (!field.matches(Regex("[A-Za-z0-9_]+"))) {
                "$sqlTemplate $field"
            } else {
                "$sqlTemplate $field = ?"
            }
            parameter(value)
        }
    }


    fun toPreparedStatement(): ConnectionAndPreparedStatement {
        val conn = connectionGetter()
        val ps = preparedStatementGetter(sql, conn)
        furtherOps.forEach { it(ps) }
        return ConnectionAndPreparedStatement(conn,ps)
    }
}


class ConnectionAndPreparedStatement(val conn: Connection, val ps: PreparedStatement)

class SelectOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter, { sql, conn ->
        conn.prepareStatement(sql)
    }, sqlTemplate)

    fun parameters(vararg parameters: Pair<String, Any?>): SelectOperation {
        builder.parameters(parameters)
        return this
    }

    fun parameter(value: Any?): SelectOperation {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): SelectOperation {
        builder.parameters(parameters)
        return this
    }

    fun parameter(parameter: Pair<String, Any?>): SelectOperation {
        builder.parameter(parameter)
        return this
    }

    @Experimental
    fun whereOptional(fieldOrTemplate: String, value: Any?): SelectOperation {
        if (value != null) {
            builder.whereOptional(fieldOrTemplate, value)
        }
        return this
    }

    fun parameter(parameter: String, value: Any?): SelectOperation {
        builder.parameter(parameter, value)
        return this
    }

    fun <T : Any> toObservable(mapper: (ResultSet) -> T) = Observable.defer {
        val cps = builder.toPreparedStatement()
        ResultSetState({ cps.ps.executeQuery() }, cps.ps, cps.conn, autoClose).toObservable(mapper)
    }

    fun <T : Any> toFlowable(mapper: (ResultSet) -> T) = Flowable.defer {
        val cps = builder.toPreparedStatement()
        ResultSetState({ cps.ps.executeQuery() }, cps.ps, cps.conn, autoClose).toFlowable(mapper)
    }

    fun <T : Any> toSingle(mapper: (ResultSet) -> T) = Single.defer {
        toObservable(mapper).singleOrError()
    }

    fun <T : Any> toMaybe(mapper: (ResultSet) -> T) = Maybe.defer {
        toObservable(mapper).singleElement()
    }

    fun toCompletable() = toFlowable { Unit }.ignoreElements()

    fun <T: Any> toPipeline(mapper: (ResultSet) -> T) = Pipeline(this, mapper = mapper)

    fun <T : Any> toSequence(mapper: (ResultSet) -> T): ResultSetSequence<T> {
        val cps = builder.toPreparedStatement()
        return ResultSetState({ cps.ps.executeQuery() }, cps.ps, cps.conn, autoClose).toSequence(mapper)
    }

    fun <T: Any> blockingFirst(mapper: (ResultSet) -> T) = toSequence(mapper).let {
        val result = it.first()
        it.close()
        result
    }
    fun <T: Any> blockingFirstOrNull(mapper: (ResultSet) -> T) = toSequence(mapper).let {
        val result = it.firstOrNull()
        it.close()
        result
    }
}


class BatchExecute<T>(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val batchSize: Int,
        val elements: Observable<T>,
        val parameterMapper: PreparedStatement.(T) -> Unit,
        val autoClose: Boolean
) {
    private val builder = PreparedStatementBuilder(connectionGetter, { sql, conn -> conn.prepareStatement(sql, RETURN_GENERATED_KEYS) }, sqlTemplate)

    fun parameter(value: Any?): BatchExecute<T> {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): BatchExecute<T> {
        builder.parameters(parameters)
        return this
    }

    fun parameter(parameter: Pair<String, Any?>): BatchExecute<T> {
        builder.parameter(parameter)
        return this
    }

    fun parameter(parameter: String, value: Any?): BatchExecute<T> {
        builder.parameter(parameter, value)
        return this
    }

    fun toObservable() = Observable.defer {

        val cps = builder.toPreparedStatement()

        cps.conn.autoCommit = false

        zip(elements, Observable.rangeLong(0L, Long.MAX_VALUE))
                .flatMap { (t, i) ->
                    cps.ps.parameterMapper(t)

                    cps.ps.addBatch()
                    if ((i + 1L) % batchSize.toLong() == 0L) {
                        Observable.fromIterable(cps.ps.executeBatch().asIterable())
                    } else {
                        Observable.empty()
                    }
                }
                .concatWith(Observable.defer { Observable.fromIterable(cps.ps.executeBatch().asIterable()) })
                .doOnComplete {
                    if (autoClose) {
                        cps.ps.close()
                        cps.conn.close()
                    }
                    cps.conn.autoCommit = true
                }
    }

    /*fun toFlowable() = Flowable.defer {

        val cps = builder.toPreparedStatement()

        cps.conn.autoCommit = false

        zip(elements, Flowable.rangeLong(0L, Long.MAX_VALUE))
                .flatMap { (t, i) ->
                    cps.ps.parameterMapper(t)

                    cps.ps.addBatch()
                    if ((i + 1L) % batchSize.toLong() == 0L) {
                        Observable.fromIterable(cps.ps.executeBatch().asIterable())
                    } else {
                        Observable.empty()
                    }
                }
                .concatWith(Observable.defer { Observable.fromIterable(cps.ps.executeBatch().asIterable()) })
                .doOnComplete {
                    if (autoClose) {
                        cps.ps.close()
                        cps.conn.close()
                    }
                    cps.conn.autoCommit = true
                }
    }*/
    private fun <T1, T2> zip(source1: Observable<T1>, source2: Observable<T2>) =
            Observable.zip(source1, source2,
                    BiFunction<T1, T2, Pair<T1, T2>> { t1, t2 -> t1 to t2 })

    private fun <T1, T2> zip(source1: Flowable<T1>, source2: Flowable<T2>) =
            Flowable.zip(source1, source2,
                    BiFunction<T1, T2, Pair<T1, T2>> { t1, t2 -> t1 to t2 })
}


class InsertOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter,{sql, conn -> conn.prepareStatement(sql, RETURN_GENERATED_KEYS)},sqlTemplate)

    fun parameter(value: Any?): InsertOperation {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): InsertOperation {
        builder.parameters(parameters)
        return this
    }
    fun parameter(parameter: Pair<String,Any?>): InsertOperation {
        builder.parameter(parameter)
        return this
    }
    fun parameter(parameter: String, value: Any?): InsertOperation {
        builder.parameter(parameter,value)
        return this
    }

    fun <T: Any> toObservable(mapper: (ResultSet) -> T) = Observable.defer {
        val cps = builder.toPreparedStatement()
        ResultSetState({
            cps.ps.executeUpdate()
            cps.ps.generatedKeys
        }, cps.ps, cps.conn, autoClose).toObservable(mapper)
    }

    fun <T: Any> toFlowable(mapper: (ResultSet) -> T) = Flowable.defer {
        val cps = builder.toPreparedStatement()
        ResultSetState({
            cps.ps.executeUpdate()
            cps.ps.generatedKeys
        }, cps.ps, cps.conn, autoClose).toFlowable(mapper)
    }

    fun <T: Any> toSingle(mapper: (ResultSet) -> T) = Single.defer {
        toObservable(mapper).singleOrError()
    }

    fun <T: Any> toMaybe(mapper: (ResultSet) -> T) = Maybe.defer {
        toObservable(mapper).singleElement()
    }

    fun toCompletable() = toFlowable { Unit }.ignoreElements()

    fun <T: Any> toPipeline(mapper: (ResultSet) -> T) = Pipeline(insertOperation = this, mapper = mapper)

    fun <T : Any> toSequence(mapper: (ResultSet) -> T): ResultSetSequence<T> {
        val cps = builder.toPreparedStatement()
        return ResultSetState({
            cps.ps.executeUpdate()
            cps.ps.generatedKeys
        }, cps.ps, cps.conn, autoClose).toSequence(mapper)
    }

    fun <T: Any> blockingFirst(mapper: (ResultSet) -> T) = toSequence(mapper).let {
        val result = it.first()
        it.close()
        result
    }
    fun <T: Any> blockingFirstOrNull(mapper: (ResultSet) -> T) = toSequence(mapper).let {
        val result = it.firstOrNull()
        it.close()
        result
    }
}

class UpdateOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter,{sql, conn -> conn.prepareStatement(sql)},sqlTemplate)

    fun parameters(vararg parameters: Pair<String,Any?>): UpdateOperation {
        builder.parameters(parameters)
        return this
    }

    fun parameter(value: Any?): UpdateOperation {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): UpdateOperation {
        builder.parameters(parameters)
        return this
    }
    fun parameter(parameter: Pair<String,Any?>): UpdateOperation {
        builder.parameter(parameter)
        return this
    }
    fun parameter(parameter: String, value: Any?): UpdateOperation {
        builder.parameter(parameter,value)
        return this
    }

    fun toSingle() = Single.defer {
        Single.just(builder.toPreparedStatement().ps.executeUpdate())
    }

    fun toCompletable() = toSingle().toCompletable()

    fun blockingGet() = builder.toPreparedStatement().ps.executeUpdate()
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

    fun <T: Any> toSequence(mapper: (ResultSet) -> T) =
            QueryIterator(this, resultSetGetter(), mapper, autoClose).let(::ResultSetSequence)

}

class  ResultSetSequence<out T>(private val queryIterator: QueryIterator<T>): Sequence<T> {
    override fun iterator() = queryIterator
    fun close() = queryIterator.close()
    val isClosed get() = queryIterator.rs.isClosed
}

class QueryIterator<out T>(val qs: ResultSetState,
                           val rs: ResultSet,
                           val mapper: (ResultSet) -> T,
                           val autoClose: Boolean
) : Iterator<T>, AutoCloseable {

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
        if (!hasNext)
            close()

        return hasNext
    }

    fun asIterable() = object: Iterable<T> {
        override fun iterator(): Iterator<T> = this@QueryIterator
    }

    override fun close() {
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


class Pipeline<T: Any>(val selectOperation: SelectOperation? = null,
                       val insertOperation: InsertOperation? = null,
                       val mapper: (ResultSet) -> T
) {
    fun toObservable(): Observable<T> = selectOperation?.toObservable(mapper) ?: insertOperation?.toObservable(mapper) ?: throw Exception("Operation must be provided")
    fun toFlowable(): Flowable<T> = selectOperation?.toFlowable(mapper) ?: insertOperation?.toFlowable(mapper) ?: throw Exception("Operation must be provided")
    fun toSingle(): Single<T> = selectOperation?.toSingle(mapper) ?: insertOperation?.toSingle(mapper) ?: throw Exception("Operation must be provided")
    fun toMaybe(): Maybe<T> = selectOperation?.toMaybe(mapper) ?: insertOperation?.toMaybe(mapper) ?: throw Exception("Operation must be provided")
    fun toSequence(): ResultSetSequence<T> = selectOperation?.toSequence(mapper) ?: insertOperation?.toSequence(mapper) ?: throw Exception("Operation must be provided")
    fun blockingFirst() = selectOperation?.blockingFirst(mapper) ?: insertOperation?.blockingFirst(mapper) ?: throw Exception("Operation must be provided")
    fun blockingFirstOrNull() = selectOperation?.blockingFirstOrNull(mapper) ?: insertOperation?.blockingFirstOrNull(mapper) ?: throw Exception("Operation must be provided")
}

fun PreparedStatement.parameters(vararg v: Any?) = v.forEachIndexed { i, v2 -> parameter(i,v2)}

fun PreparedStatement.parameter(pos: Int, argVal: Any?) {
    when (argVal) {
        null -> setObject(pos + 1, null)
        is UUID -> setObject(pos + 1, argVal)
        is Int -> setInt(pos + 1, argVal)
        is String -> setString(pos + 1, argVal)
        is Double -> setDouble(pos + 1, argVal)
        is Boolean -> setBoolean(pos + 1, argVal)
        is Float -> setFloat(pos + 1, argVal)
        is Long -> setLong(pos + 1, argVal)
        is LocalTime -> setTime(pos + 1, java.sql.Time.valueOf(argVal))
        is LocalDate -> setDate(pos + 1, java.sql.Date.valueOf(argVal))
        is java.sql.Date -> setDate(pos + 1, argVal)
        is java.sql.Timestamp -> setTimestamp(pos + 1, argVal)
        is java.sql.Time -> setTime(pos + 1, argVal)
        is java.util.Date -> setDate(pos + 1, java.sql.Date(argVal.time))
        is LocalDateTime -> setTimestamp(pos + 1, java.sql.Timestamp.valueOf(argVal))
        is BigDecimal -> setBigDecimal(pos + 1, argVal)
        is InputStream -> setBinaryStream(pos + 1, argVal)
        is Enum<*> -> setObject(pos + 1, argVal)
    }
}


fun ResultSet.asList() =  (1..this.metaData.columnCount).asSequence().map {
    this.getObject(it)
}.toList()

fun ResultSet.asMap() =  (1..this.metaData.columnCount).asSequence().map {
    metaData.getColumnName(it) to getObject(it)
}.toMap()
