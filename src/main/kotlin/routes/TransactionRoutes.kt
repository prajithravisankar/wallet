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
    }
}