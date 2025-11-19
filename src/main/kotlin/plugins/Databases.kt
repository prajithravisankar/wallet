package com.example.plugins

import com.example.Database // Import the Database object from your other file

fun configureDatabases() {
    Database.init()
    DemoDataSeeder.reseedDemoDataOnApplicationStartup()
}
    