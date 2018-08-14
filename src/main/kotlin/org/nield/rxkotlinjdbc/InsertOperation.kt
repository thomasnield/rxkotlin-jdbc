package org.nield.rxkotlinjdbc

import io.reactivex.Flowable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement

class InsertOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter, { sql, conn -> conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) }, sqlTemplate)

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