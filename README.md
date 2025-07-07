# 🛫 Flight Booking Data ETL Project

This project demonstrates how to handle nested JSON flight booking data using PySpark — parsing, flattening, and writing to Parquet files.

| Developed BY | LastUpdate | Youtube Chennel Name |
| ------------ | ---------- | ---- |
| Vignesan Saravanan | 07-07-2025 | VijaQuick |

## Dataset
The `fightdataset.json` file contains flight bookings with nested arrays and structs.

# Architecture

                         ┌──────────────────────────┐
                         │    fightdataset.json     │
                         │ (Nested JSON Input File) │
                         └────────────┬─────────────┘
                                      │
                    Read using        │ .option("multiline", True)
                                      ▼
                          ┌────────────────────┐
                          │  PySpark DataFrame │
                          │  (Raw JSON Loaded) │
                          └─────────┬──────────┘
                                    │
                  ┌─────────────────┴───────────────────┐
         Explode  │                                     │ Select Struct Fields
         'flights' array                        Extract 'customer' Struct
                  ▼                                     ▼
     ┌────────────────────┐                 ┌────────────────────────┐
     │ Flattened Flights  │                 │   Customer Details     │
     │ booking_id, from,  │                 │ name, email, phone     │
     │ to, depart, arrive │                 └────────────────────────┘
     └──────────┬─────────┘                            │
                │                                      │
                ▼                                      ▼
       ┌──────────────────────┐              ┌──────────────────────┐
       │ Write to Parquet     │              │ Write to Parquet     │
       │ /flight_data/        │              │ /customer_data/      │
       └────────┬─────────────┘              └────────┬─────────────┘
                │                                      │
                ▼                                      ▼
       ┌──────────────────────┐              ┌──────────────────────┐
       │ Stored in Data Lake  │              │ Stored in Data Lake  │
       │ or DBFS              │              │ or DBFS              │
       └──────────────────────┘              └──────────────────────┘

Each booking contains:
- Customer information
- Array of flights (multi-leg journeys)
- Booking status and timestamp

## Features
- Parse nested JSON with multiline support
- Explode flight array into multiple rows
- Extract nested customer struct fields
- Write flattened data to partitioned Parquet
- Clean, modular PySpark code

## Output
- `/mnt/tables/flight_data/` → booking and flight info  
- `/mnt/tables/customer_data/` → customer info  

## 📸 Sample Output

| Booking ID | From     | To       | Departure | Arrival  |
|------------|----------|----------|-----------|----------|
| BKG001     | Chennai  | Delhi    | 09:00     | 11:30    |

## Technologies Used

- Apache Spark (PySpark)

- Databricks FileStore

- Parquet Format

- JSON Schema Inference


