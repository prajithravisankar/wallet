package com.example.plugins

import at.favre.lib.crypto.bcrypt.BCrypt
import com.example.Database
import java.math.BigDecimal
import java.sql.PreparedStatement
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

/**
 *
 * Concurrency primitives used:
 *  - ReentrantLock               -> protects critical section for inserting users.
 *  - ReentrantReadWriteLock      -> protects shared userId list access.
 *  - Thread + join               -> explicit thread management.
 */
object DemoDataSeeder {

    // -------------------------------------------------------------------------
    // Configuration constants
    // -------------------------------------------------------------------------

    private const val NUMBER_OF_DEMO_USERS = 10
    private const val NUMBER_OF_TRANSACTIONS_PER_CATEGORY_AND_USER = 5

    private val transactionAndBudgetCategories = listOf(
        "Food",
        "Utilities",
        "Transportation",
        "Personal",
        "Family",
        "Entertainment",
        "Subscriptions",
        "Miscellaneous"
    )

    // -------------------------------------------------------------------------
    // Concurrency primitives and shared data
    // -------------------------------------------------------------------------

    /**
     * This lock ensures that inserting users is a single-threaded
     * critical section (even though we currently do not spawn multiple
     * threads for user creation)
     */
    private val userInsertLock = ReentrantLock()

    /**
     * Shared list of all demo user ids. This represents the "source of truth"
     * for which users exist after seeding.
     *
     * Access to this list is protected by a ReadWrite lock so that:
     *  - We can safely add userIds while seeding users.
     *  - We can safely read the full list when starting concurrent work.
     */
    private val sharedDemoUserIdList: MutableList<Int> = mutableListOf()
    private val sharedDemoUserIdListReadWriteLock = ReentrantReadWriteLock()

    /**
     * This method is meant to be called once from configureDatabases() during
     * application startup.
     */
    fun reseedDemoDataOnApplicationStartup() {
        println("===================== [DemoDataSeeder] Starting demo data reseed pipeline... ============================")

        deleteAllExistingRowsFromAllDomainTables()

        createDemoUsersSequentiallyAndStoreUserIdsInSharedList()

        val safeSnapshotOfUserIds =
            obtainThreadSafeSnapshotOfAllDemoUserIdsFromSharedList()

        startConcurrentDataGenerationThreadsForAllUsersAndWaitForCompletion(
            safeSnapshotOfUserIds
        )

        println("===================== [DemoDataSeeder] Finished demo data reseed pipeline... ============================")
    }

    /**
     * Deletes all rows from transactions, budgets, and users.
     */
    private fun deleteAllExistingRowsFromAllDomainTables() {
        Database.connect().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(
                    """
                    TRUNCATE TABLE transactions, budgets, users
                    RESTART IDENTITY CASCADE;
                    """.trimIndent()
                )
            }
        }
        println("[DemoDataSeeder] All tables truncated and identities reset.")
    }

    /**
     * Creates a fixed number of demo users, one-by-one, inside a locked
     * critical section and stores their generated user_ids in a shared list.
     */
    private fun createDemoUsersSequentiallyAndStoreUserIdsInSharedList() {
        // example of a single-threaded critical section
        userInsertLock.withLock {
            Database.connect().use { connection ->
                connection.autoCommit = false

                val insertUserSql = """
                    INSERT INTO users (first_name, last_name, email, password)
                    VALUES (?, ?, ?, ?)
                    RETURNING user_id
                """.trimIndent()

                connection.prepareStatement(insertUserSql).use { preparedStatement ->
                    for (index in 1..NUMBER_OF_DEMO_USERS) {
                        val newUserId = insertSingleDemoUserAndReturnGeneratedId(
                            index,
                            preparedStatement
                        )
                        addNewDemoUserIdToSharedListInThreadSafeWay(newUserId)
                    }
                }

                connection.commit()
            }
        }
        println("[DemoDataSeeder] Demo users created and stored in shared list.")
    }

    /**
     * Inserts a single demo user using the provided PreparedStatement, and
     * returns the generated user_id received from the database.
     */
    private fun insertSingleDemoUserAndReturnGeneratedId(
        userIndex: Int,
        preparedStatement: PreparedStatement
    ): Int {
        val firstName = "User$userIndex"
        val lastName = "Demo"
        val email = "user$userIndex@example.com"
        val rawPassword = "password$userIndex"

        val hashedPassword = BCrypt
            .withDefaults()
            .hashToString(12, rawPassword.toCharArray())

        preparedStatement.setString(1, firstName)
        preparedStatement.setString(2, lastName)
        preparedStatement.setString(3, email)
        preparedStatement.setString(4, hashedPassword)

        preparedStatement.executeQuery().use { resultSet ->
            resultSet.next()
            val generatedUserId = resultSet.getInt("user_id")
            println("[DemoDataSeeder] Created demo user: id=$generatedUserId, email=$email")
            return generatedUserId
        }
    }

    /**
     * Adds the newly created user id into the shared list in a thread-safe way
     * using the write lock of the ReadWrite lock.
     */
    private fun addNewDemoUserIdToSharedListInThreadSafeWay(newUserId: Int) {
        val writeLock = sharedDemoUserIdListReadWriteLock.writeLock()
        writeLock.lock()
        try {
            sharedDemoUserIdList.add(newUserId)
        } finally {
            writeLock.unlock()
        }
    }

    /**
     * Returns a copy of the shared user id list using the read lock, so the
     * caller can safely iterate without worrying about concurrent modification.
     */
    private fun obtainThreadSafeSnapshotOfAllDemoUserIdsFromSharedList(): List<Int> {
        val readLock = sharedDemoUserIdListReadWriteLock.readLock()
        readLock.lock()
        try {
            return sharedDemoUserIdList.toList()
        } finally {
            readLock.unlock()
        }
    }

    /**
     * For every user id:
     *  - Creates one thread PER CATEGORY to generate transactions.
     *  - Creates one thread to generate all budgets.
     *
     * Then this function waits (join) for all created threads to finish.
     */
    private fun startConcurrentDataGenerationThreadsForAllUsersAndWaitForCompletion(
        demoUserIds: List<Int>
    ) {
        val allWorkerThreads = mutableListOf<Thread>()

        for (userId in demoUserIds) {
            // One transaction thread per category for this user
            for (category in transactionAndBudgetCategories) {
                val transactionThread =
                    createAndStartTransactionGeneratorThreadForSingleUserAndSingleCategory(
                        userId,
                        category
                    )
                allWorkerThreads.add(transactionThread)
            }

            // One budget thread per user
            val budgetThread =
                createAndStartBudgetGeneratorThreadForSingleUserAcrossAllCategories(userId)
            allWorkerThreads.add(budgetThread)
        }

        waitForAllWorkerThreadsToCompleteBeforeContinuingStartup(allWorkerThreads)
    }

    /**
     * Creates and immediately starts a thread that will generate
     * transactions for a single (userId, category) pair.
     */
    private fun createAndStartTransactionGeneratorThreadForSingleUserAndSingleCategory(
        userId: Int,
        category: String
    ): Thread {
        val threadName = "Tx-Generator-User$userId-Category-$category"
        val thread = Thread({
            generateRandomDemoTransactionsForSingleUserAndSingleCategory(userId, category)
        }, threadName)

        thread.start()
        return thread
    }

    /**
     * Creates and immediately starts a thread that will generate all budgets
     * for a single user across all categories.
     */
    private fun createAndStartBudgetGeneratorThreadForSingleUserAcrossAllCategories(
        userId: Int
    ): Thread {
        val threadName = "Budget-Generator-User$userId"
        val thread = Thread({
            generateDemoBudgetsForSingleUserAcrossAllCategories(userId)
        }, threadName)

        thread.start()
        return thread
    }

    /**
     * Waits for all generated worker threads (transactions + budgets) to
     * complete before the application continues startup.
     */
    private fun waitForAllWorkerThreadsToCompleteBeforeContinuingStartup(
        workerThreads: List<Thread>
    ) {
        workerThreads.forEach { thread ->
            try {
                thread.join()
            } catch (interrupted: InterruptedException) {
                println("[DemoDataSeeder] Thread ${thread.name} was interrupted: ${interrupted.message}")
                Thread.currentThread().interrupt()
            }
        }
        println("[DemoDataSeeder] All worker threads finished successfully.")
    }

    /**
     * Generates a fixed number of demo transactions for a single (userId, category)
     * pair. This function is executed inside its own thread.
     */
    private fun generateRandomDemoTransactionsForSingleUserAndSingleCategory(
        userId: Int,
        category: String
    ) {
        Database.connect().use { connection ->
            val insertTransactionSql = """
                INSERT INTO transactions (
                    user_id,
                    title,
                    category,
                    sub_category,
                    transaction_type,
                    amount,
                    date,
                    description,
                    location
                )
                VALUES (?, ?, ?, ?, ?::transaction_type, ?, ?::timestamp with time zone, ?, ?)
            """.trimIndent()

            connection.prepareStatement(insertTransactionSql).use { preparedStatement ->
                val nowUtc = OffsetDateTime.now(ZoneOffset.UTC)

                repeat(NUMBER_OF_TRANSACTIONS_PER_CATEGORY_AND_USER) { index ->
                    val transactionIndex = index + 1

                    val isExpenseTransaction = (transactionIndex % 3 != 0)
                    val transactionType = if (isExpenseTransaction) "expense" else "income"

                    val transactionAmount = if (isExpenseTransaction) {
                        BigDecimal((10..100).random())
                    } else {
                        BigDecimal((100..500).random())
                    }

                    val daysAgo = (0..60).random().toLong()
                    val transactionDate = nowUtc.minusDays(daysAgo)

                    preparedStatement.setInt(1, userId)
                    preparedStatement.setString(2, "$category transaction $transactionIndex")
                    preparedStatement.setString(3, category)
                    preparedStatement.setString(4, "General")
                    preparedStatement.setString(5, transactionType)
                    preparedStatement.setBigDecimal(6, transactionAmount)
                    preparedStatement.setString(7, transactionDate.toString())
                    preparedStatement.setString(8, "Sample $category transaction for demo user $userId")
                    preparedStatement.setString(9, "Demo Location")

                    preparedStatement.addBatch()
                }

                preparedStatement.executeBatch()
            }
        }

        println(
            "[DemoDataSeeder] Thread ${Thread.currentThread().name} " +
                    "finished generating transactions for userId=$userId, category=$category"
        )
    }

    /**
     * Generates daily, weekly, monthly and yearly budgets for all categories
     * for a single user. This function is executed inside its own thread.
     */
    private fun generateDemoBudgetsForSingleUserAcrossAllCategories(userId: Int) {
        val today = LocalDate.now()

        val startOfWeek = today.with(DayOfWeek.MONDAY)
        val endOfWeek = startOfWeek.plusDays(6)

        val startOfMonth = today.withDayOfMonth(1)
        val endOfMonth = startOfMonth.plusMonths(1).minusDays(1)

        val startOfYear = today.withDayOfYear(1)
        val endOfYear = startOfYear.plusYears(1).minusDays(1)

        Database.connect().use { connection ->
            val insertBudgetSql = """
                INSERT INTO budgets (
                    user_id,
                    category,
                    sub_category,
                    budget_limit,
                    period_type,
                    start_date,
                    end_date,
                    description
                )
                VALUES (?, ?, ?, ?, ?::period_type, ?::date, ?::date, ?)
            """.trimIndent()

            connection.prepareStatement(insertBudgetSql).use { preparedStatement ->
                for (category in transactionAndBudgetCategories) {
                    addSingleBudgetRowToBatchForCategoryAndPeriod(
                        preparedStatement,
                        userId,
                        category,
                        "daily",
                        BigDecimal("50.00"),
                        today,
                        today,
                        "Daily $category budget for demo user $userId"
                    )

                    addSingleBudgetRowToBatchForCategoryAndPeriod(
                        preparedStatement,
                        userId,
                        category,
                        "weekly",
                        BigDecimal("200.00"),
                        startOfWeek,
                        endOfWeek,
                        "Weekly $category budget for demo user $userId"
                    )

                    addSingleBudgetRowToBatchForCategoryAndPeriod(
                        preparedStatement,
                        userId,
                        category,
                        "monthly",
                        BigDecimal("800.00"),
                        startOfMonth,
                        endOfMonth,
                        "Monthly $category budget for demo user $userId"
                    )

                    addSingleBudgetRowToBatchForCategoryAndPeriod(
                        preparedStatement,
                        userId,
                        category,
                        "yearly",
                        BigDecimal("9600.00"),
                        startOfYear,
                        endOfYear,
                        "Yearly $category budget for demo user $userId"
                    )
                }

                preparedStatement.executeBatch()
            }
        }

        println(
            "[DemoDataSeeder] Thread ${Thread.currentThread().name} " +
                    "finished generating budgets for userId=$userId"
        )
    }

    /**
     * Adds a single budget row into the PreparedStatement batch.
     */
    private fun addSingleBudgetRowToBatchForCategoryAndPeriod(
        preparedStatement: PreparedStatement,
        userId: Int,
        category: String,
        periodType: String,
        budgetLimit: BigDecimal,
        startDateInclusive: LocalDate,
        endDateInclusive: LocalDate,
        description: String
    ) {
        preparedStatement.setInt(1, userId)
        preparedStatement.setString(2, category)
        preparedStatement.setString(3, "General")
        preparedStatement.setBigDecimal(4, budgetLimit)
        preparedStatement.setString(5, periodType)
        preparedStatement.setString(6, startDateInclusive.toString())
        preparedStatement.setString(7, endDateInclusive.toString())
        preparedStatement.setString(8, description)

        preparedStatement.addBatch()
    }
}
