package com.example.routes

import com.example.Database
import com.example.models.Transaction
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.math.BigDecimal

fun Route.transactionRouting() {
    route("/transactions") {
        post {
            val transaction = call.receive<Transaction>()

            println("RAW RECEIVED JSON: $transaction")

            val connection = Database.connect()

            val sql = """
                INSERT INTO transactions (user_id, title, category, sub_category, transaction_type, amount, date, description, location)
                VALUES (?, ?, ?, ?, ?::transaction_type, ?, ?::timestamp with time zone, ?, ?)
            """.trimIndent()

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)

                statement.setInt(1, transaction.userId)
                statement.setString(2, transaction.title)
                statement.setString(3, transaction.category)
                statement.setString(4, transaction.subCategory)
                statement.setString(5, transaction.transactionType)
                statement.setBigDecimal(6, BigDecimal(transaction.amount))
                statement.setString(7, transaction.date)
                statement.setString(8, transaction.description)
                statement.setString(9, transaction.location)

                statement.executeUpdate()
            }

            call.respond(HttpStatusCode.Created, "Transaction stored successfully")
        }

        get("{userId}") {
            val userId = call.parameters["userId"]?.toIntOrNull()

            if (userId == null) {
                call.respond(HttpStatusCode.BadRequest, "invalid user id")
                return@get
            }

            val connection = Database.connect()
            val sql = """
                SELECT transaction_id, user_id, title, category, sub_category, transaction_type, 
                        amount, date, description, location, created_at
                FROM transactions 
                WHERE user_id = ? 
                ORDER BY date DESC
            """.trimIndent()

            val transactions = mutableListOf<Transaction>()
            connection.use { conn ->
                val statement = conn.prepareStatement(sql)
                statement.setInt(1, userId)
                val resultSet = statement.executeQuery()
                while (resultSet.next()) {
                    val currentTransaction = Transaction(
                        transactionId = resultSet.getInt("transaction_id"),
                        userId = resultSet.getInt("user_id"),
                        title = resultSet.getString("title"),
                        category = resultSet.getString("category"),
                        subCategory = resultSet.getString("sub_category"),
                        transactionType = resultSet.getString("transaction_type"),
                        amount = resultSet.getBigDecimal("amount")?.toPlainString() ?: "0",
                        date = resultSet.getTimestamp("date")?.toInstant()?.toString()?: resultSet.getString("date") ?: "",
                        description = resultSet.getString("description"),
                        location = resultSet.getString("location"),
                        createdAt = resultSet.getTimestamp("created_at")?.toInstant()?.toString() ?: ""
                    )
                    transactions.add(currentTransaction)
                }
            }

            call.respond(HttpStatusCode.OK, transactions)
        }

        put("{id}") {
            val transactionId = call.parameters["id"]?.toIntOrNull()
            if (transactionId == null) {
                call.respond(HttpStatusCode.BadRequest, "invalid transaction id")
                return@put
            }

            val updatedTransaction = call.receive<Transaction>()

            val connection = Database.connect()
            /**
             * transaction_type = ?::transaction_type means:
             * Take the parameter value (?) and cast it to the custom PostgreSQL enum type
             * transaction_type (which only allows 'expense' or 'income')
             */
            val sql = """
                UPDATE transactions
                SET user_id = ?, 
                    title = ?, 
                    category = ?, 
                    sub_category = ?, 
                    transaction_type = ?::transaction_type, 
                    amount = ?, 
                    date = ?::timestamp with time zone, 
                    description = ?, 
                    location = ?
                WHERE transaction_id = ?
            """.trimIndent()

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)
                statement.setInt(1, updatedTransaction.userId)
                statement.setString(2, updatedTransaction.title)
                statement.setString(3, updatedTransaction.category)
                statement.setString(4, updatedTransaction.subCategory)
                statement.setString(5, updatedTransaction.transactionType)
                statement.setBigDecimal(6, BigDecimal(updatedTransaction.amount))
                statement.setString(7, updatedTransaction.date)
                statement.setString(8, updatedTransaction.description)
                statement.setString(9, updatedTransaction.location)
                statement.setInt(10, transactionId)

                val numberOfRowsAffected = statement.executeUpdate()
                if (numberOfRowsAffected == 0) {
                    call.respond(HttpStatusCode.NotFound, "transaction not found for transaction id to update")
                } else {
                    call.respond(HttpStatusCode.OK, "transaction with transaction id updated successfully")
                }
            }
        }

        delete("{id}") {
            val transactionId = call.parameters["id"]?.toIntOrNull()
            if (transactionId == null) {
                call.respond(HttpStatusCode.BadRequest, "invalid transaction id")
                return@delete
            }

            val connection = Database.connect()
            val sql = "DELETE FROM transactions WHERE transaction_id = ?"

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)
                statement.setInt(1, transactionId)
                val numberOfRowsAffected = statement.executeUpdate()
                if (numberOfRowsAffected == 0) {
                    call.respond(HttpStatusCode.NotFound, "transaction not found")
                } else {
                    call.respond(HttpStatusCode.NoContent)
                }
            }
        }
    }
}