# Wallet Backend API

## Project Overview

This is a **Personal Finance Management System** built as a class project for **COMP 4411 - Programming Languages** at Lakehead University. The project demonstrates various programming paradigms including Object-Oriented, Procedural, and Concurrent programming using Kotlin and the Ktor framework.

## DEVELOPMENT DOCS: documentation.md
Please refer to [documentation.md](./documentation.md)

This project was created using the [Ktor Project Generator](https://start.ktor.io).

## Technology Stack

### Backend Framework & Language
- **Kotlin** - Primary programming language
- **Ktor 3.0+** - Asynchronous web framework for building REST APIs
- **Gradle** - Build automation and dependency management

### Database
- **PostgreSQL** - Relational database management system
- **JDBC** - Direct database connectivity without ORM

### Security & Utilities
- **BCrypt** (at.favre.lib:bcrypt:0.10.2) - Password hashing
- **kotlinx.serialization** - JSON serialization/deserialization
- **Logback** - Logging framework

### Server
- **Netty** - High-performance asynchronous event-driven network application framework

## Programming Paradigms Demonstrated

### 1. Object-Oriented Programming (OOP)

The project uses OOP principles extensively with data classes, singleton objects, and encapsulation.

**Example - Data Models:**
```kotlin
// User.kt - Data class with properties
@Serializable
data class User(
    val userId: Int? = null,
    val firstName: String,
    val lastName: String,
    val email: String,
    val password: String,
    val createdAt: String? = null,
    val updatedAt: String? = null,
)

// Transaction.kt - Domain model
@Serializable
data class Transaction(
    val transactionId: Int? = null,
    val userId: Int,
    val title: String,
    val category: String,
    val transactionType: String,
    val amount: String,
    val date: String
)
```

**Example - Repository Pattern:**
```kotlin
// UserRepository.kt - Encapsulation of data access logic
class UserRepository {
    fun createUser(first: String, last: String, email: String, hashedPassword: String): Int {
        return DbHelper.query(
            sql = SqlBlueprints.INSERT_USER,
            prepare = { ps ->
                ps.setString(1, first)
                ps.setString(2, last)
                ps.setString(3, email)
                ps.setString(4, hashedPassword)
            },
            map = { rs ->
                rs.next()
                rs.getInt("user_id")
            }
        )
    }
}
```

**Example - Singleton Object Pattern:**
```kotlin
// Database.kt - Singleton object for database connection
object Database {
    fun connect(): Connection {
        val url = "jdbc:postgresql://localhost:5432/postgres"
        val user = "postgres"
        val password = "mysecretpassword"
        return DriverManager.getConnection(url, user, password)
    }
    
    fun init() {
        // Initialize database tables
    }
}
```

### 2. Procedural Programming

The project uses procedural approaches for sequential operations and data processing.

**Example - Routing Configuration:**
```kotlin
// Routing.kt - Procedural route setup
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
```

**Example - Database Operations:**
```kotlin
// TransactionRoutes.kt - Procedural database insert
post {
    val transaction = call.receive<Transaction>()
    val connection = Database.connect()
    
    val sql = """
        INSERT INTO transactions (user_id, title, category, transaction_type, amount, date)
        VALUES (?, ?, ?, ?::transaction_type, ?, ?::timestamp with time zone)
    """.trimIndent()
    
    connection.use { conn ->
        val statement = conn.prepareStatement(sql)
        statement.setInt(1, transaction.userId)
        statement.setString(2, transaction.title)
        statement.setString(3, transaction.category)
        statement.setString(5, transaction.transactionType)
        statement.setBigDecimal(6, BigDecimal(transaction.amount))
        statement.executeUpdate()
    }
    
    call.respond(HttpStatusCode.Created, "Transaction stored successfully")
}
```

### 3. Concurrent Programming

The project demonstrates thread-based concurrency with proper synchronization mechanisms.

**Example - Thread-Safe Demo Data Generation:**
```kotlin
// DemoDataSeeder.kt - Concurrent data seeding with thread safety
object DemoDataSeeder {
    // Concurrency primitives
    private val userInsertLock = ReentrantLock()
    private val userIdListRWLock = ReentrantReadWriteLock()
    private val sharedUserIdList = mutableListOf<Int>()
    
    // Thread-safe write operation
    private fun addUserIdThreadSafely(newId: Int) {
        userIdListRWLock.writeLock().withLock {
            sharedUserIdList.add(newId)
        }
    }
    
    // Thread-safe read operation
    private fun safelyGetAllUserIdsSnapshot(): List<Int> {
        userIdListRWLock.readLock().withLock {
            return sharedUserIdList.toList()
        }
    }
    
    // Creating concurrent worker threads
    private fun createConcurrentWorkerThreadsForAllUsers(userIds: List<Int>) {
        val allThreads = mutableListOf<Thread>()
        
        for (userId in userIds) {
            // One transaction thread per category
            for (category in categories) {
                val t = Thread({
                    generateTransactions(userId, category)
                }, "Tx-User$userId-$category")
                t.start()
                allThreads.add(t)
            }
            
            // One budget thread per user
            val b = Thread({
                generateBudgets(userId)
            }, "Budget-User$userId")
            b.start()
            allThreads.add(b)
        }
        
        // Wait for all threads to complete
        allThreads.forEach { it.join() }
    }
}
```

### 4. Functional Programming Elements

Kotlin's functional programming features are used throughout the project.

**Example - Higher-Order Functions:**
```kotlin
// DbHelper.kt - Generic query function using lambdas
fun <T> query(
    sql: String,
    prepare: (PreparedStatement) -> Unit,
    map: (ResultSet) -> T
): T {
    Database.connect().use { connection ->
        connection.prepareStatement(sql).use { ps ->
            prepare(ps)
            ps.executeQuery().use { rs ->
                return map(rs)
            }
        }
    }
}
```

## Key Features

- **User Management**: Registration, login with BCrypt password hashing, profile updates
- **Transaction Tracking**: Create, read, update, and delete financial transactions
- **Budget Management**: Set and monitor spending budgets by category
- **RESTful API**: Clean REST endpoints for all operations
- **Thread-Safe Demo Data**: Concurrent data seeding with proper synchronization
- **PostgreSQL Integration**: Direct JDBC usage with custom SQL queries
- **CORS Support**: Cross-origin resource sharing for frontend integration

## API Endpoints

### Users
- `POST /users` - Create new user
- `GET /users/login` - Authenticate user
- `PUT /users/{id}` - Update user profile

### Transactions
- `POST /transactions` - Create transaction
- `GET /transactions/{userId}` - Get all transactions for a user
- `PUT /transactions/{id}` - Update transaction
- `DELETE /transactions/{id}` - Delete transaction

### Budgets
- `POST /budgets` - Create budget
- `GET /budgets/{userId}` - Get all budgets for a user
- `PUT /budgets/{id}` - Update budget
- `DELETE /budgets/{id}` - Delete budget

## Database Schema

The application uses PostgreSQL with the following custom types and tables:

### Custom ENUM Types
- `transaction_type`: `'expense' | 'income'`
- `period_type`: `'daily' | 'weekly' | 'monthly' | 'yearly'`

### Tables
- **users**: User accounts with authentication
- **transactions**: Financial transaction records
- **budgets**: Budget limits and tracking

## Useful Links

- [Ktor Documentation](https://ktor.io/docs/home.html)
- [Ktor GitHub page](https://github.com/ktorio/ktor)
- The [Ktor Slack chat](https://app.slack.com/client/T09229ZC6/C0A974TJ9). You'll need to [request an invite](https://surveys.jetbrains.com/s3/kotlin-slack-sign-up) to join.

## Features

Here's a list of features included in this project:

| Name                                                                   | Description                                                                        |
| ------------------------------------------------------------------------|------------------------------------------------------------------------------------ |
| [Routing](https://start.ktor.io/p/routing)                             | Provides a structured routing DSL                                                  |
| [OpenAPI](https://start.ktor.io/p/openapi)                             | Serves OpenAPI documentation                                                       |
| [CORS](https://start.ktor.io/p/cors)                                   | Enables Cross-Origin Resource Sharing (CORS)                                       |
| [kotlinx.serialization](https://start.ktor.io/p/kotlinx-serialization) | Handles JSON serialization using kotlinx.serialization library                     |
| [Content Negotiation](https://start.ktor.io/p/content-negotiation)     | Provides automatic content conversion according to Content-Type and Accept headers |
| [Postgres](https://start.ktor.io/p/postgres)                           | Adds Postgres database to your application                                         |

## Building & Running

To build or run the project, use one of the following tasks:

| Task                                    | Description                                                          |
| -----------------------------------------|---------------------------------------------------------------------- |
| `./gradlew test`                        | Run the tests                                                        |
| `./gradlew build`                       | Build everything                                                     |
| `./gradlew buildFatJar`                 | Build an executable JAR of the server with all dependencies included |
| `./gradlew buildImage`                  | Build the docker image to use with the fat JAR                       |
| `./gradlew publishImageToLocalRegistry` | Publish the docker image locally                                     |
| `./gradlew run`                         | Run the server                                                       |
| `./gradlew runDocker`                   | Run using the local docker image                                     |

If the server starts successfully, you'll see the following output:

```
2024-12-04 14:32:45.584 [main] INFO  Application - Application started in 0.303 seconds.
2024-12-04 14:32:45.682 [main] INFO  Application - Responding at http://0.0.0.0:8080
```

