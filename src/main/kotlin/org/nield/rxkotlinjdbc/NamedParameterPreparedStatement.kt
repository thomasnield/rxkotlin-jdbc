package org.nield.rxkotlinjdbc

import java.sql.PreparedStatement

class NamedParameterPreparedStatement(val mappedParameters: Map<String,List<Int>>,
                                      val cps: ConnectionAndPreparedStatement
) : PreparedStatement by cps.ps {

    val conn = cps.conn
    val ps = cps.ps

    fun parameter(parameter: Pair<String,Any?>) {
        parameter(parameter.first, parameter.second)
    }
    fun parameter(parameter: String, value: Any?) {
        (mappedParameters[":$parameter"] ?: throw Exception("Parameter $parameter not found!}"))
                .asSequence()
                .forEach { i -> parameter(i, value) }
    }
}