package org.nield.rxkotlinjdbc

import io.reactivex.Flowable
import io.reactivex.Observable
import java.sql.Connection
import java.sql.PreparedStatement

fun Connection.execute(sqlTemplate: String) = UpdateOperation(
        sqlTemplate = sqlTemplate,
        connectionGetter = { this },
        autoClose = false
)

fun Connection.select(sqlTemplate: String)  =
        SelectOperation(
                sqlTemplate = sqlTemplate,
                connectionGetter = { this },
                autoClose = false
        )

/**
 * Executes an INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun Connection.insert(insertSQL: String)  =
        InsertOperation(
                sqlTemplate = insertSQL,
                connectionGetter = { this },
                autoClose = false
        )

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> Connection.batchExecute(sqlTemplate: String,
                                         elements: Observable<T>,
                                         batchSize: Int,
                                         parameterMapper: PreparedStatement.(T) -> Unit,
                                         autoClose: Boolean = false
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsObservable = elements,
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this },
        autoClose = autoClose
)

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> Connection.batchExecute(sqlTemplate: String,
                                         elements: Flowable<T>,
                                         batchSize: Int,
                                         parameterMapper: NamedParameterPreparedStatement.(T) -> Unit,
                                         autoClose: Boolean = false
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsFlowable = elements,
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this },
        autoClose = autoClose
)

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> Connection.batchExecute(sqlTemplate: String,
                                         elements: Iterable<T>,
                                         batchSize: Int,
                                         parameterMapper: NamedParameterPreparedStatement.(T) -> Unit,
                                         autoClose: Boolean = false
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsIterable = elements,
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this },
        autoClose = autoClose
)

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> Connection.batchExecute(sqlTemplate: String,
                                         elements: Sequence<T>,
                                         batchSize: Int,
                                         parameterMapper: PreparedStatement.(T) -> Unit,
                                         autoClose: Boolean = false
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsIterable = elements.asIterable(),
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this },
        autoClose = autoClose
)