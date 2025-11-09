package com.example

import com.example.routes.transactionRouting
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.plugins.openapi.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import java.sql.Connection
import java.sql.DriverManager

fun Application.configureRouting() {
    routing {
        // activation the transaction routes
        transactionRouting()

        // example route
        get("/") {
            call.respondText("Hello World!")
        }
    }
}
