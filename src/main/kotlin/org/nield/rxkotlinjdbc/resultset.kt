package org.nield.rxkotlinjdbc

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

fun ResultSet.asList() =  (1..this.metaData.columnCount).asSequence().map {
    this.getObject(it)
}.toList()

fun ResultSet.asMap() =  (1..this.metaData.columnCount).asSequence().map {
    metaData.getColumnName(it) to getObject(it)
}.toMap()


class ConnectionAndPreparedStatement(val conn: Connection, val ps: PreparedStatement)


class  ResultSetSequence<out T>(private val queryIterator: QueryIterator<T>): Sequence<T> {
    override fun iterator() = queryIterator
    fun close() = queryIterator.close()
    val isClosed get() = queryIterator.rs.isClosed
}