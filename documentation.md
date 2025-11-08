## Local Development Setup 

> start: prajith ravisankar - date: nov 8, 2025 - time: 11:00 AM.

To run this project on your local machine, you will need to have a PostgreSQL database running. We use Docker to ensure a consistent environment for all developers.

### 1. Prerequisites

- Make sure you have [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running on your computer.

### 2. Start the Database

Open your terminal and run the following command to start a PostgreSQL container:

- verify that we have required dependencies in the build.gradle.kts
  - `implementation(libs.postgresql)`
- open docker desktop
- using docker to setup postgre sql to run postgre sql from my computer
  - `docker run --name my-wallet-db -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres`
  - you will see something like this :
    - ![img.png](temp_images/img.png)
  - now we have created our personal isolated database server that is running in our computer background
  - to verify if our database is running `docker ps
    - ![img.png](temp_images/verify_new_container_running.png)

### 3. Database connection
- refer Databases.kt to understand how we establish connection to postgre sql. 
  - `DriverManager.getConnection(url, user, password)` this line establishes the connection with postgre sql by calling the connection driver. 
- whenever we start our web server(s) we want it to do 2 things: 
  - connect to the database using the Database.connect() function.
  - create our tables for the server to use if the tables don't exist already. 
    - we created custom types for transactions and budgets and used it inside corresponding tables
    - created list of string of sql commands
    - and are executing each of these sql commands in order in a loop with the help of the `connection.use {conn -> }` method
- These are 2 Databases.kt, one in the package com.example, one inside com.example.plugins. 
  - the one in the plugins folder is used by Application.kt to call the initialization function in the Databases.kt in the com.example package. 

### 4. errors faced
- expected errors, the very first time we run the server it is expected for Database.init() function connected to our empty database. 
- our try catch block caught the error and printed it in the console. 
  - 1.Your app tries to create transaction_type. It fails because it exists. Your catch block prints "Error executing...".
  - 2.Your app tries to create period_type. It fails because it exists. Your catch block prints "Error executing...".
  - 3.Your app checks for the users table. It exists, so it does nothing.
  - 4.Your app checks for the transactions table. It exists, so it does nothing.
  - 5.Your app checks for the budgets table. It exists, so it does nothing.
  - 6.The server finishes starting.

Finally, when verifying if the tables are created properly use this step: ![img.png](temp_images/is_tables_created_successfully_docker_image_check.png)

> end: prajith ravisankar - date: nov 8, 2025 - time: 3:10 PM.

---
