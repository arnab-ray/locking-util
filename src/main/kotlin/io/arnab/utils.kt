package io.arnab

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope

suspend fun <A, B> Iterable<A>.parallelMap(func: suspend (A) -> B): List<B> {
    val iterable = this
    return coroutineScope {
        iterable.chunked(5)
            .map {iterableSubset ->
                iterableSubset.map { async { func(it) } }.awaitAll()
            }.flatten()
    }
}