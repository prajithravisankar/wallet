package com.example.routes

import com.example.Database
import com.example.models.Transaction
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.math.BigDecimal

// we are extending the Ktor's Route class with our own transactionRouteing function
fun Route.transactionRouting() {
    // creates grouping of all the routes related to "/transactions" endpoint
    route("/transactions") {
        // POST endpoint
        post {
            // call.receive is from server.request library
            // the data class from Transaction.kt takes care of the JSON formatting automatically
            // because of the @Serializable annotation. The JSON response we get will automatically
            // converted to the underlying Transaction data class below.
            val transaction = call.receive<Transaction>()

            // get a connection the the database, it is open, the use {..} block will close it
            // after opening the connection below.
            val connection = Database.connect()

            // we are using safe place holders here (?) - prevents SQL injection attack
            // :: handles type conversions automatically kotlin -> sql types (we have custom type)
            val sql = """
                INSERT INTO transactions (user_id, title, category, sub_category, transaction_type, amount, date, description, location)
                VALUES (?, ?, ?, ?, ?::transaction_type, ?, ?::timestamp with time zone, ?, ?)
            """.trimIndent()

            // automatically closes the connection after executing the statements
            // meaning: Open a database connection, prepare this SQL statement safely, and when done,
            // automatically close the connection — even if something goes wrong.
            connection.use { conn ->
                // preparedStatement = compiled SQL statements ready to get values inserted in place
                // of "?" placeholders.
                val statement = conn.prepareStatement(sql)

                // filling the ? placeholders one by one
                statement.setInt(1, transaction.userId)
                statement.setString(2, transaction.title)
                statement.setString(3, transaction.category)
                statement.setString(4, transaction.subCategory)
                statement.setString(5, transaction.transactionType)
                statement.setBigDecimal(6, BigDecimal(transaction.amount))
                statement.setString(7, transaction.date)
                statement.setString(8, transaction.description)
                statement.setString(9, transaction.location)

                // execute the query to insert the data
                statement.executeUpdate()
            }

            call.respond(HttpStatusCode.Created, "Transaction stored successfully")
        }

        // look for get request to "/transaction/someNumber" and name the someNumber to userId
        // Example: /transactions/1 here the userId is 1
        get("{userId}") {
            // ktor captures the value from URL path and its always a string, we need to convert it to int
            val userId = call.parameters["userId"]?.toIntOrNull()

            if (userId == null) {
                call.respond(HttpStatusCode.BadRequest, "invalid user id")
                return@get // this means exit only from current lambda inner function the get {...}
                // not the entire outer function.
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
                statement.setInt(1, userId) // replace the first ? placeholder with userId
                val resultSet = statement.executeQuery() // resultSet is like a cursor for a table where we can loop through all the records row by row
                while (resultSet.next()) {
                    // while the cursor is not at the end of the table, get the current transaction (current row)
                    // and create a Transaction object from it and add it to the transactions list and return it.
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

                    // resultSet.getTimestamp("date")?.toInstant()?.toString()?: resultSet.getString("date") ?: "",
                    // resultSet.getTimestamp("date")?.toInstant() -> call toInstant only if resultSet.getTimestamp("date") not null
                    // resultSet.getTimestamp("date")?.toInstant()?.toString() -> call toString only if resultSet.getTimestamp("date")?.toInstant() not null
                    // resultSet.getTimestamp("date")?.toInstant()?.toString() ?: resultSet.getString("date") -> if this (resultSet.getTimestamp("date")?.toInstant()?.toString()) is null, use this (resultSet.getString("date"))
                    // resultSet.getTimestamp("date")?.toInstant()?.toString()?: resultSet.getString("date") ?: "" -> if this is null (resultSet.getTimestamp("date")?.toInstant()?.toString()?: resultSet.getString("date")) use this ("")

                    transactions.add(currentTransaction)
                }
            }

            call.respond(HttpStatusCode.OK, transactions)
        }
    }
}