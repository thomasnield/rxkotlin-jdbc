package org.nield.rxkotlinjdbc

import io.reactivex.Single
import java.sql.Connection

class UpdateOperation(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val autoClose: Boolean
) {

    val builder = PreparedStatementBuilder(connectionGetter, { sql, conn -> conn.prepareStatement(sql) }, sqlTemplate)

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
        val cps = builder.toPreparedStatement()
        ResultSetState({
            cps.ps.executeUpdate()
            cps.ps.generatedKeys
        }, cps.ps, cps.conn, autoClose).toObservable { it }.count()
    }

    fun toCompletable() = toSingle().ignoreElement()

    fun blockingGet(): Unit {
        val cps = builder.toPreparedStatement()
        ResultSetState({
            cps.ps.executeUpdate()
            cps.ps.generatedKeys
        }, cps.ps, cps.conn, autoClose).toSequence { 0 }.first()
    }
}