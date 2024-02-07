package io.arnab

object DBConfig {
    internal fun getDBConfigKey(): String? = "mongodb://localhost:27017"

    internal fun getDBName(): String? = "db-name"

    internal fun getDBCollectionName(): String? = "db-collection-name"
}