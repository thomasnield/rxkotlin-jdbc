package org.nield.rxkotlinjdbc

import io.reactivex.Flowable
import io.reactivex.Maybe
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.annotations.Experimental
import java.sql.Connection
import java.sql.ResultSet

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