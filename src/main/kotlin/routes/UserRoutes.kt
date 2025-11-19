package com.example.routes

import com.example.Database
import com.example.models.User
import io.ktor.http.HttpStatusCode
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import at.favre.lib.crypto.bcrypt.BCrypt


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
//                statement.setString(4, user.password)
                val hashedPassword = BCrypt.withDefaults().hashToString(12, user.password.toCharArray())
                statement.setString(4, hashedPassword)


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

                    val result = BCrypt.verifyer().verify(password.toCharArray(), storedPassword)

                    if (result.verified) {
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

        put("/{id}") {
            val userId = call.parameters["id"]?.toIntOrNull()
                ?: return@put call.respond(HttpStatusCode.BadRequest, "Invalid user ID")

            val updateRequest = call.receive<User>()

            val sql = """
                UPDATE users
                SET first_name = ?, last_name = ?, email = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """.trimIndent()

            Database.connect().use { conn ->
                val statement = conn.prepareStatement(sql)
                statement.setString(1, updateRequest.firstName)
                statement.setString(2, updateRequest.lastName)
                statement.setString(3, updateRequest.email)
                statement.setInt(4, userId)

                val rows = statement.executeUpdate()
                if (rows == 0) {
                    return@put call.respond(HttpStatusCode.NotFound, "User not found")
                }
            }

            call.respond(HttpStatusCode.OK, "User profile updated")
        }

        put("/{id}/password") {

            val userId = call.parameters["id"]?.toIntOrNull()
                ?: return@put call.respond(HttpStatusCode.BadRequest, "Invalid user ID")

            val request = call.receive<Map<String, String>>()
            val oldPassword = request["oldPassword"] ?: return@put call.respond(HttpStatusCode.BadRequest)
            val newPassword = request["newPassword"] ?: return@put call.respond(HttpStatusCode.BadRequest)

            Database.connect().use { conn ->

                // Fetch existing password hash
                val selectSql = "SELECT password FROM users WHERE user_id = ?"
                val storedHash: String? = conn.prepareStatement(selectSql).use { stmt ->
                    stmt.setInt(1, userId)
                    val rs = stmt.executeQuery()
                    if (rs.next()) rs.getString("password") else null
                }

                if (storedHash == null)
                    return@put call.respond(HttpStatusCode.NotFound, "User not found")

                // Verify old password
                val verified = BCrypt.verifyer().verify(oldPassword.toCharArray(), storedHash).verified
                if (!verified)
                    return@put call.respond(HttpStatusCode.Unauthorized, "Incorrect old password")

                // Hash new password
                val newHash = BCrypt.withDefaults().hashToString(12, newPassword.toCharArray())

                // Update password
                val updateSql = """
                    UPDATE users SET password = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                """
                conn.prepareStatement(updateSql).use { stmt ->
                    stmt.setString(1, newHash)
                    stmt.setInt(2, userId)
                    stmt.executeUpdate()
                }
            }

            call.respond(HttpStatusCode.OK, "Password updated successfully")
        }


    }
}