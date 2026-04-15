# Billing Data Pipeline to Palantir Foundry

## Overview
This repository contains a simulated ETL (Extract, Transform, Load) data pipeline designed to extract massive billing records from an on-premise SQL Server, anonymize sensitive financial data, and ingest the payload into Palantir Foundry for Big Data analysis.

## Architecture
The pipeline is divided into two microservices to optimize resources:
1. **Data Extraction & Transformation (Python):** Connects to the SQL Server, runs extraction queries, obfuscates client names for privacy compliance, and serializes the data into a highly compressed `.parquet` format using Pandas.
2. **Data Ingestion API (C# / .NET):** An asynchronous service that picks up the generated Parquet file and securely uploads it to the Palantir Foundry REST API endpoints using Bearer token authentication.

## Technologies Used
* **Languages:** Python 3.9, C# (.NET Core)
* **Libraries/Packages:** Pandas, SQLAlchemy, PyArrow, System.Net.Http
* **Data Storage:** SQL Server, Parquet
* **Integration:** Palantir Foundry (Simulated REST API)

## Security & Compliance
* Implemented Data Obfuscation to mask PII (Personally Identifiable Information).
* No credentials or API keys are hardcoded in the source code (simulated via Environment Variables).
