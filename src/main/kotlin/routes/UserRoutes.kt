package com.example.routes

import com.example.Database
import com.example.models.User
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Route.userRouting() {
    route("/users") {
        post {
            val user = call.receive<User>()

            val connection = Database.connect()

            val sql = """
                INSERT INTO users (first_name, last_name, email, password)
                VALUES (?, ?, ?, ?)
            """.trimIndent()

            connection.use { conn ->
                val statement = conn.prepareStatement(sql)

                statement.setString(1, user.firstName)
                statement.setString(2, user.lastName)
                statement.setString(3, user.email)
                statement.setString(4, user.password)

                statement.executeUpdate()
            }

            call.respond(HttpStatusCode.Created, "User created successfully")
        }

        get("/login") {
            val email = call.parameters["email"]
            val password = call.parameters["password"]

            if (email == null || password == null) {
                call.respond(HttpStatusCode.BadRequest, "Email and password are required")
                return@get
            }

            val connection = Database.connect()
            val sql = """
                SELECT user_id, first_name, last_name, email, password, created_at, updated_at
                FROM users
                WHERE email = ?
            """.trimIndent()

            var user: User? = null
            connection.use { conn ->
                val statement = conn.prepareStatement(sql)
                statement.setString(1, email)
                val resultSet = statement.executeQuery()

                if (resultSet.next()) {
                    val storedPassword = resultSet.getString("password")

                    if (storedPassword == password) {
                        user = User(
                            userId = resultSet.getInt("user_id"),
                            firstName = resultSet.getString("first_name"),
                            lastName = resultSet.getString("last_name"),
                            email = resultSet.getString("email"),
                            password = "",
                            createdAt = resultSet.getTimestamp("created_at")?.toInstant()?.toString() ?: "",
                            updatedAt = resultSet.getTimestamp("updated_at")?.toInstant()?.toString() ?: ""
                        )
                    }
                }
            }

            if (user != null) {
                call.respond(HttpStatusCode.OK, user)
            } else {
                call.respond(HttpStatusCode.Unauthorized, "Invalid email or password")
            }
        }
    }
}