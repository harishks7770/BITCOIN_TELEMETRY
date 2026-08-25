# Real-Time Bitcoin Data Streaming Pipeline

A containerized real-time streaming pipeline that ingests live Bitcoin (`BTCUSDT`) trade events from the Binance WebSocket API, remaps case-sensitive JSON keys, streams messages to Azure Event Hubs, and processes them in a Microsoft Fabric KQL Database using KQL Update Policies.

---

## 🏗️ Architecture

```text
┌───────────────────────┐
│ Binance WebSocket API │
└───────────┬───────────┘
            │  (Live BTCUSDT Trades)
            ▼
┌───────────────────────┐
│ Dockerized Python App │  --> (app.py: Field Cleanup & Normalization)
└───────────┬───────────┘
            │  (AMQP Protocol)
            ▼
┌───────────────────────┐
│   Azure Event Hubs    │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  Fabric Eventstream   │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  Fabric KQL Database  │  --> (Transformed via KQL Update Policy)
└───────────────────────┘
