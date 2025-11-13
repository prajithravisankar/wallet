package com.example.routes

import com.example.Database
import com.example.models.Budget
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import java.math.BigDecimal

fun Route.budgetRouting() {
    // group all the endpoints for budgets together
    route("/budgets") {
        // POST /budgets -> creates a new budget ultimately should end up in the database
        post {
            val budget = call.receive<Budget>()
            val connection = Database.connect()

            // creating sql with placeholders will be filled in the later parts.
            // budget id and created at will be created by postgres itself
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
    }
}