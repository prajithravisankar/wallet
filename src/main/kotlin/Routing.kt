package com.example

import com.example.routes.budgetRouting
import com.example.routes.transactionRouting
import com.example.routes.userRouting
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Application.configureRouting() {
    routing {
        userRouting()
        transactionRouting()
        budgetRouting()

        get("/") {
            call.respondText("Hello World!")
        }
    }
}
