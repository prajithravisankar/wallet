package com.example

import java.sql.Connection
import java.sql.DriverManager

object Database {
    fun connect(): Connection {
//        val url = "jdbc:postgresql://localhost:5432/postgres"
//        val user = "postgres"
//        val password = "12345Prasri2004$$"

        val url = "jdbc:postgresql://localhost:5433/walletdb"
        val user = "walletuser"
        val password = "walletpass"


        return DriverManager.getConnection(url, user, password)
    }

    fun init() {
        val sqlStatements = listOf(
            "CREATE TYPE transaction_type AS ENUM ('expense', 'income');",
            "CREATE TYPE period_type AS ENUM ('daily', 'weekly', 'monthly', 'yearly');",

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
                    location VARCHAR(100), 
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

        val connection = connect()
        connection.use { conn ->
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