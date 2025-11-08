// we have 2 Databases.kt, but that is fine because we are putting this
// inside another package and seperate our concers.
package com.example.plugins

import com.example.Database // Import the Database object from your other file
import io.ktor.server.application.*

fun Application.configureDatabases() {
    // This is the function that gets called from Application.kt.
    // We initialize our database schema here.
    Database.init()
}
    