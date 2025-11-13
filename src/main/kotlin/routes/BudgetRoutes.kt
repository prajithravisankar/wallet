package com.example.routes

import com.example.Database
import com.example.models.Budget
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
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
    }
}