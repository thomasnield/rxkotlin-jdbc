package org.nield.rxkotlinjdbc

import java.sql.ResultSet
import java.util.concurrent.atomic.AtomicBoolean

class QueryIterator<out T>(val qs: ResultSetState? = null,
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
        qs?.statement?.close()
        if (autoClose)
            qs?.connection?.close()
    }
    fun cancel() {
        cancelled.set(true)
    }

    private fun excecuteCancel() {
        rs.close()
        qs?.statement?.close()
        if (autoClose)
            qs?.connection?.close()
    }
}