package org.nield.rxkotlinjdbc

import io.reactivex.Flowable
import io.reactivex.Observable
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

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