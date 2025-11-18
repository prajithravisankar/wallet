package com.example.models

import kotlinx.serialization.Serializable

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