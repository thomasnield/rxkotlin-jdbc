package org.nield.rxkotlinjdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.PreparedStatement
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*

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
        is Date -> setDate(pos + 1, java.sql.Date(argVal.time))
        is LocalDateTime -> setTimestamp(pos + 1, java.sql.Timestamp.valueOf(argVal))
        is BigDecimal -> setBigDecimal(pos + 1, argVal)
        is InputStream -> setBinaryStream(pos + 1, argVal)
        is Enum<*> -> setObject(pos + 1, argVal)
    }
}