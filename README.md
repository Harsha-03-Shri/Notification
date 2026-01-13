# Notification Microservice

## Overview

This project implements a **notification microservice** using **FastAPI**, **Kafka**, and **Cassandra**. It allows sending notifications via **Email**, **FCM**, and **APN**. Failures are handled via a **Dead Letter Queue (DLQ)** and persisted into Cassandra.

---

## Architecture
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/802e80c7-b43d-46db-9ee2-61339e0f07ef" />


## Components

### 1. API Server

- **Framework**: FastAPI
- Fetches user info and templates from Cassandra
- Pushes messages to Kafka topics
- Endpoints:
  - `/notify/payment-success`
  - `/notify/course-completed`
  - `/notify/assignment-submitted`
  - `/addUser`

### 2. Worker Node

- **Language**: Python
- **Kafka Client**: aiokafka
- Consumers:
  - Email → sends via Gmail SMTP using App Password
  - FCM → sends push notification
  - APN → sends push notification
- On failure, messages go to **DLQ**

### 3. DLQ Consumer

- Reads **DLQ Kafka topic**
- Pushes failure messages into Cassandra table: `failure_dlq`

### 4. Cassandra DB

- **Keyspace**: `notification`
- Tables:
  - `users`: stores user info
  - `notification_templates`: stores templates
  - `failure_dlq`: stores failed notifications

---
