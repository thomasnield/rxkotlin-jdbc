package org.nield.rxkotlinjdbc

import io.reactivex.annotations.Experimental
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.concurrent.atomic.AtomicInteger

class PreparedStatementBuilder(
        val connectionGetter: () -> Connection,
        val preparedStatementGetter: (String, Connection) -> PreparedStatement,
        var sqlTemplate: String

) {

    private val namelessParameterIndex = AtomicInteger(-1)
    val sql: String by lazy { sqlTemplate.replace(parameterRegex,"?") }
    val furtherOps: MutableList<(PreparedStatement) -> Unit> = mutableListOf()

    companion object {
        private val parameterRegex = Regex(":[_A-Za-z0-9]+")
    }

    val mappedParameters by lazy {
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
        return ConnectionAndPreparedStatement(conn, ps)
    }
}