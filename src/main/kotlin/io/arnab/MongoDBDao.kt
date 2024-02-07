package io.arnab

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase

class MongoDBDao(
    configKey: String,
    private val databaseName: String,
    private val collectionName: String,
) {

    val mongoClient: MongoClient by lazy {
        MongoClients.create(configKey)
    }

    private val mongoDatabase: MongoDatabase by lazy {
        mongoClient.getDatabase(databaseName)
    }

    val mongoCollection by lazy {
        mongoDatabase.getCollection(collectionName)
    }

    suspend fun createItem()
}