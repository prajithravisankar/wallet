package com.example

import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*

fun Application.configureSerialization() {
    // ContentNegotitation plugin lets us automatically convert between ktor objects <-> json objects and vice versa
    install(ContentNegotiation) {
        json()
    }
}
