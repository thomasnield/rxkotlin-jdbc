package org.nield.rxkotlinjdbc

import io.reactivex.Flowable
import io.reactivex.Observable
import java.sql.PreparedStatement
import javax.sql.DataSource

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> DataSource.batchExecute(sqlTemplate: String,
                                          elements: Observable<T>,
                                          batchSize: Int,
                                          parameterMapper: PreparedStatement.(T) -> Unit,
                                          autoClose: Boolean = true
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsObservable = elements,
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this.connection },
        autoClose = autoClose
)

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> DataSource.batchExecute(sqlTemplate: String,
                                          elements: Flowable<T>,
                                          batchSize: Int,
                                          parameterMapper: PreparedStatement.(T) -> Unit,
                                          autoClose: Boolean = true
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsFlowable = elements,
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this.connection },
        autoClose = autoClose
)

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> DataSource.batchExecute(sqlTemplate: String,
                                          elements: Iterable<T>,
                                          batchSize: Int,
                                          parameterMapper: PreparedStatement.(T) -> Unit,
                                          autoClose: Boolean = true
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsIterable = elements,
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this.connection },
        autoClose = autoClose
)

/**
 * Executes a batched INSERT operation and returns the generated keys as a single-field `ResultSet`
 */
fun <T> DataSource.batchExecute(sqlTemplate: String,
                                          elements: Sequence<T>,
                                          batchSize: Int,
                                          parameterMapper: PreparedStatement.(T) -> Unit,
                                          autoClose: Boolean = true
) = BatchExecute(
        sqlTemplate = sqlTemplate,
        elementsIterable = elements.asIterable(),
        batchSize = batchSize,
        parameterMapper = parameterMapper,
        connectionGetter = { this.connection },
        autoClose = autoClose
)

fun DataSource.execute(sqlTemplate: String) = UpdateOperation(
        sqlTemplate = sqlTemplate,
        connectionGetter = { connection },
        autoClose = true
)

fun DataSource.select(sqlTemplate: String)  =
        SelectOperation(
                sqlTemplate = sqlTemplate,
                connectionGetter = { this.connection },
                autoClose = true
        )

fun DataSource.insert(insertSQL: String) =
        InsertOperation(
                sqlTemplate = insertSQL,
                connectionGetter = { this.connection },
                autoClose = true
        )