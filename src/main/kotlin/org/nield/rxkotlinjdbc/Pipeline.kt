package org.nield.rxkotlinjdbc

import io.reactivex.*
import java.sql.ResultSet

class Pipeline<T: Any>(val selectOperation: SelectOperation? = null,
                       val insertOperation: InsertOperation? = null,
                       val mapper: (ResultSet) -> T
) {
    fun toObservable(): Observable<T> = selectOperation?.toObservable(mapper) ?: insertOperation?.toObservable(mapper) ?: throw Exception("Operation must be provided")
    fun toFlowable(): Flowable<T> = selectOperation?.toFlowable(mapper) ?: insertOperation?.toFlowable(mapper) ?: throw Exception("Operation must be provided")
    fun toCompletable(): Completable = selectOperation?.toCompletable() ?: insertOperation?.toCompletable() ?: throw Exception("Operation must be provided")
    fun toSingle(): Single<T> = selectOperation?.toSingle(mapper) ?: insertOperation?.toSingle(mapper) ?: throw Exception("Operation must be provided")
    fun toMaybe(): Maybe<T> = selectOperation?.toMaybe(mapper) ?: insertOperation?.toMaybe(mapper) ?: throw Exception("Operation must be provided")
    fun toSequence(): ResultSetSequence<T> = selectOperation?.toSequence(mapper) ?: insertOperation?.toSequence(mapper) ?: throw Exception("Operation must be provided")
    fun blockingFirst() = selectOperation?.blockingFirst(mapper) ?: insertOperation?.blockingFirst(mapper) ?: throw Exception("Operation must be provided")
    fun blockingFirstOrNull() = selectOperation?.blockingFirstOrNull(mapper) ?: insertOperation?.blockingFirstOrNull(mapper) ?: throw Exception("Operation must be provided")
}