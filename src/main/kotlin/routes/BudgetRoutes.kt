package com.example.routes

import com.example.Database
import com.example.models.Budget
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import java.math.BigDecimal

fun Route.budgetRouting() {
    route("/budgets") {
        post {
            val budget = call.receive<Budget>()
            val connection = Database.connect()

            val sql = """
                INSERT INTO budgets (
                    user_id, category, sub_category, budget_limit, period_type, start_date, 
                    end_date, description
                )
                VALUES (?, ?, ?, ?, ?::period_type, ?::date, ?::date, ?)
            """.trimIndent()

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)

                statement.setInt(1, budget.userId)
                statement.setString(2, budget.category)
                statement.setString(3, budget.subCategory)
                statement.setBigDecimal(4, BigDecimal(budget.budgetLimit)) // here is where we are actually converting it from string to decimal
                statement.setString(5, budget.periodType)
                statement.setString(6, budget.startDate)
                statement.setString(7, budget.endDate)
                statement.setString(8, budget.description)

                // this is where we actually insert a row in the postgres.
                statement.executeUpdate()
            }

            call.respond(HttpStatusCode.Created, "budget created successfully")

        }

        // GET /budgets/{userId} -> fetch all the budgets for a particular user
        get("{userId}") {
            val userId = call.parameters["userId"]?.toIntOrNull()
            if (userId == null) {
                call.respond(HttpStatusCode.BadRequest, "invalid user id")
                return@get
            }

            val connection = Database.connect()

            val sql = """
                SELECT budget_id, user_id, category, sub_category, budget_limit, period_type, start_date,
                end_date, description, created_at
                FROM budgets
                WHERE user_id = ?
                ORDER BY start_date DESC
            """.trimIndent()

            val budgets = mutableListOf<Budget>()

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)
                statement.setInt(1, userId)
                val resultSet = statement.executeQuery()
                while (resultSet.next()) {
                    val currentBudget = Budget(
                        budgetId = resultSet.getInt("budget_id"),
                        userId = resultSet.getInt("user_id"),
                        category = resultSet.getString("category"),
                        subCategory = resultSet.getString("sub_category"),
                        budgetLimit = resultSet.getBigDecimal("budget_limit")?.toPlainString() ?: "0",
                        periodType = resultSet.getString("period_type"),
                        startDate = resultSet.getDate("start_date")?.toString() ?: "",
                        endDate = resultSet.getDate("end_date")?.toString() ?: "",
                        description = resultSet.getString("description"),
                        createdAt = resultSet.getTimestamp("created_at")?.toInstant()?.toString() ?: ""
                    )

                    budgets.add(currentBudget)
                }
            }

            call.respond(HttpStatusCode.OK, budgets)
        }

        // PUT /budgets/{id} -> update an existing budget
        put("{id}") {
            val budgetId = call.parameters["id"]?.toIntOrNull()
            if (budgetId == null) {
                call.respond(HttpStatusCode.BadRequest, "invalid budget id")
                return@put
            }

            val updatedBudget = call.receive<Budget>()

            val connection = Database.connect()

            val sql = """
                UPDATE budgets
                SET user_id = ?, category = ?, sub_category = ?, budget_limit = ?, period_type = ?::period_type, 
                start_date = ?::date, end_date = ?::date, description = ?
                WHERE budget_id = ?
            """.trimIndent()

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)

                statement.setInt(1, updatedBudget.userId)
                statement.setString(2, updatedBudget.category)
                statement.setString(3, updatedBudget.subCategory)
                statement.setBigDecimal(4, BigDecimal(updatedBudget.budgetLimit))
                statement.setString(5, updatedBudget.periodType)
                statement.setString(6, updatedBudget.startDate)
                statement.setString(7, updatedBudget.endDate)
                statement.setString(8, updatedBudget.description)
                statement.setInt(9, budgetId)

                val rowsUpdated = statement.executeUpdate()
                if (rowsUpdated == 0) {
                    call.respond(HttpStatusCode.NotFound, "budget not found for budget id to update")
                } else {
                    call.respond(HttpStatusCode.OK, "budget updated successfully")
                }
            }
        }

        // DELETE /budgets/{id} -> delete an existing budget
        delete("{id}") {
            val budgetId = call.parameters["id"]?.toIntOrNull()
            if (budgetId == null) {
                call.respond(HttpStatusCode.BadRequest, "invalid budget id")
                return@delete
            }

            val connection = Database.connect()

            val sql = "DELETE FROM budgets WHERE budget_id = ?"

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)
                statement.setInt(1, budgetId)
                val rowsDeleted = statement.executeUpdate()
                if (rowsDeleted == 0) {
                    call.respond(HttpStatusCode.NotFound, "budget not found")
                } else {
                    call.respond(HttpStatusCode.NoContent)
                }
            }
        }
    }
}