package com.example.models

import kotlinx.serialization.Serializable

@Serializable
data class Transaction(
    val transactionId: Int? = null,
    val userId: Int,
    val title: String,
    val category: String,
    val subCategory: String? = null,
    val transactionType: String,
    val amount: String,
    val date: String,
    val description: String? = null,
    val location: String? = null,
    val createdAt: String? = null
)