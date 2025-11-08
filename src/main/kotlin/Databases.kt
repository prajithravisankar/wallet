// Make sure this package name matches the one your other files are in
package com.example

import java.sql.Connection
import java.sql.DriverManager

object Database {
    fun connect(): Connection {
        // The address of our database running in Docker
        val url = "jdbc:postgresql://localhost:5432/postgres"

        // The username for the database. "postgres" is the default.
        val user = "postgres"

        // The password we set in our 'docker run' command
        val password = "mysecretpassword"

        // This line uses the PostgreSQL driver to establish a connection
        return DriverManager.getConnection(url, user, password)
    }

    // this function will setup our database whenever we start our server
    fun init() {
        val sqlStatements = listOf(
            // custom types
            "CREATE TYPE transaction_type AS ENUM ('expense', 'income');", // postgre sql needs this for custom type declare it with TYPE
            "CREATE TYPE period_type AS ENUM ('daily', 'weekly', 'monthly', 'yearly');",

            // create tables if they don't exist in the database
            // here SERIAL - auto incrementing numbers
            """
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL, 
                    last_name VARCHAR(255) NOT NULL, 
                    email VARCHAR(255) UNIQUE NOT NULL, 
                    password VARCHAR(255) NOT NULL, 
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """.trimIndent(),
            """
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id SERIAL PRIMARY KEY, 
                    user_id INT NOT NULL REFERENCES users(user_id),
                    title VARCHAR(255) NOT NULL, 
                    category VARCHAR(50) NOT NULL, 
                    sub_category VARCHAR(50), 
                    transaction_type transaction_type NOT NULL, 
                    amount DECIMAL(10, 2) NOT NULL, 
                    date TIMESTAMP WITH TIME ZONE NOT NULL, 
                    description TEXT, 
                    location VARCHAR(10), 
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """.trimIndent(),
            """
                CREATE TABLE IF NOT EXISTS budgets (
                    budget_id SERIAL PRIMARY KEY, 
                    user_id INT NOT NULL REFERENCES users(user_id), 
                    category VARCHAR(50) NOT NULL, 
                    sub_category VARCHAR(50), 
                    budget_limit DECIMAL(10, 2) NOT NULL, 
                    period_type period_type NOT NULL, 
                    start_date DATE NOT NULL, 
                    end_date DATE NOT NULL, 
                    description TEXT, 
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """.trimIndent()
        )

        val connection = connect() // we call the connection to the postgresql
        // below use automatically closes the block {}
        // conn is a connection object, we can use it to send sql commands
        connection.use { conn ->
            // this statement object lets us send sql commands to the database
            val statement = conn.createStatement()
            sqlStatements.forEach { sql ->
                try {
                    statement.execute(sql.trimIndent())
                } catch (e: Exception) {
                    println("Error executing SQL statement in Databases.kt: $sql")
                }
            }
        }
    }
}