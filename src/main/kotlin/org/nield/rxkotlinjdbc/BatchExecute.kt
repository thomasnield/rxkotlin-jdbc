package org.nield.rxkotlinjdbc

import io.reactivex.BackpressureStrategy
import io.reactivex.Flowable
import io.reactivex.Observable
import io.reactivex.functions.BiFunction
import java.sql.Connection
import java.sql.Statement

class BatchExecute<T>(
        sqlTemplate: String,
        connectionGetter: () -> Connection,
        val batchSize: Int,
        val elementsObservable: Observable<T>? = null,
        val elementsFlowable: Flowable<T>? = null,
        val elementsIterable: Iterable<T>? = null,
        val parameterMapper: NamedParameterPreparedStatement.(T) -> Unit,
        val autoClose: Boolean
) {

    init {
        // check for only one source of elements was provided
        sequenceOf(elementsFlowable, elementsObservable, elementsIterable).filterNotNull().count()
                .let {
                    when {
                        it == 0 -> throw Exception("Must provide elements for BatchExecute")
                        it != 1 -> throw Exception("Only provide one source of elements for BatchExecute")
                        else -> {}
                    }
                }
    }

    private val builder = PreparedStatementBuilder(connectionGetter, { sql, conn -> conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) }, sqlTemplate)

    fun parameter(value: Any?): BatchExecute<T> {
        builder.parameter(value)
        return this
    }

    fun parameters(vararg parameters: Any?): BatchExecute<T> {
        builder.parameters(parameters)
        return this
    }

    fun parameter(parameter: Pair<String, Any?>): BatchExecute<T> {
        builder.parameter(parameter)
        return this
    }

    fun parameter(parameter: String, value: Any?): BatchExecute<T> {
        builder.parameter(parameter, value)
        return this
    }

    fun toCompletable() = toObservable().ignoreElements()

    fun toObservable() = Observable.defer {

        val cps = NamedParameterPreparedStatement(builder.mappedParameters, builder.toPreparedStatement())

        cps.conn.autoCommit = false

        zip(elementsObservable
                ?:elementsFlowable?.toObservable()
                ?:elementsIterable?.let { Observable.fromIterable(it) }
                ?: throw Exception("No elements were provided"),

                Observable.rangeLong(0L, Long.MAX_VALUE)
        )
        .flatMap { (t, i) ->
            cps.parameterMapper(t)

            cps.ps.addBatch()
            if ((i + 1L) % batchSize.toLong() == 0L) {
                Observable.fromIterable(cps.ps.executeBatch().asIterable())
            } else {
                Observable.empty()
            }
        }
        .concatWith(Observable.defer { Observable.fromIterable(cps.ps.executeBatch().asIterable()) })
        .doOnComplete {
            if (autoClose) {
                cps.ps.close()
                cps.conn.close()
            }
            cps.conn.autoCommit = true
        }
    }

    fun toFlowable() = Flowable.defer {

        val cps = NamedParameterPreparedStatement(builder.mappedParameters, builder.toPreparedStatement())
        cps.conn.autoCommit = false

        zip(elementsFlowable?:
            elementsIterable?.let { Flowable.fromIterable(it) }?:
            elementsObservable?.toFlowable(BackpressureStrategy.BUFFER) ?: throw Exception("No elements provided"),
             Flowable.rangeLong(0L, Long.MAX_VALUE)
            ).flatMap { (t, i) ->
                cps.parameterMapper(t)

                cps.addBatch()
                if ((i + 1L) % batchSize.toLong() == 0L) {
                    Flowable.fromIterable(cps.executeBatch().asIterable())
                } else {
                    Flowable.empty()
                }
            }
            .concatWith(Flowable.defer { Flowable.fromIterable(cps.executeBatch().asIterable()) })
            .doOnComplete {
                if (autoClose) {
                    cps.close()
                    cps.conn.close()
                }
                cps.conn.autoCommit = true
            }
    }

    fun toSequence(): Sequence<Int> {
        val cps = NamedParameterPreparedStatement(builder.mappedParameters, builder.toPreparedStatement())

        cps.conn.autoCommit = false

        return (elementsIterable?:throw Exception("An Iterable or Sequence must be provided as input to use toSequence()") )
         .asSequence()
         .withIndex()
        .flatMap { (i,t) ->
            cps.parameterMapper(t)

            cps.ps.addBatch()
            if ((i + 1L) % batchSize.toLong() == 0L) {
                cps.ps.executeBatch().asSequence()
            } else {
                emptySequence()
            }
        }
        .plus(
            generateSequence {
                val result = cps.ps.executeBatch()
                if (autoClose) {
                    cps.ps.close()
                    cps.conn.close()
                    cps.conn.autoCommit = true
                }
                result.asIterable()
            }.take(1).flatMap { it.asSequence() }
        )
    }

    private fun <T1, T2> zip(source1: Observable<T1>, source2: Observable<T2>) =
            Observable.zip(source1, source2,
                    BiFunction<T1, T2, Pair<T1, T2>> { t1, t2 -> t1 to t2 })

    private fun <T1, T2> zip(source1: Flowable<T1>, source2: Flowable<T2>) =
            Flowable.zip(source1, source2,
                    BiFunction<T1, T2, Pair<T1, T2>> { t1, t2 -> t1 to t2 })
}