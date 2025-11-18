package com.example.models

import kotlinx.serialization.Serializable

@Serializable
data class Budget(
    val budgetId: Int? = null,
    val userId: Int,
    val category: String,
    val subCategory: String? = null,
    val budgetLimit: String,
    val periodType: String,
    val startDate: String,
    val endDate: String,
    val description: String? = null,
    val createdAt: String? = null
)
