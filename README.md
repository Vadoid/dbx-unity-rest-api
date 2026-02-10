# 🌌 Databricks Unity Catalog Explorer (dbx-explore)

![CI](https://github.com/Vadoid/dbx-unity-rest-api/actions/workflows/ci.yml/badge.svg)
![Release](https://img.shields.io/github/v/release/Vadoid/dbx-unity-rest-api)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/Vadoid/dbx-unity-rest-api)





A powerful, interactive CLI tool for exploring Databricks Unity Catalog metadata and data. Built with Go and the official Databricks SDK, it offers a seamless "Wizard Mode" to navigate your data assets without writing SQL.

## Features

- **Interactive "Wizard" Mode**: Navigate Catalogs -> Schemas -> Tables with arrow keys.
- **Pure REST API**: Uses `Statement Execution` API for data queries. No heavy ODBC/JDBC drivers required.
- **Warehouse Selection**: 
  - Automatically discovers your best SQL Warehouse.
  - Supports **Serverless** detection (⚡).
  - Allows switching engines on the fly.
- **Unified Authentication**:
  - Auto-login prompt.
  - Browser-based token generation.
  - Securely saves credentials to `.env`.
- **Deep Exploration**:
  - **Sample Data**: View actual rows (`SELECT * LIMIT 5`).
  - **Extended Metadata**: View Owner, Storage Location, Format, and Properties.
- **Rich UI**: Color-coded output, bold headers, and intuitive navigation.

## Installation

### Prerequisites
- Go 1.21 or higher
- A Databricks Workspace (AWS/Azure/GCP)

### Build from Source
```bash
# 1. Clone the repository
git clone https://github.com/your-repo/dbx-explore.git
cd dbx-explore

# 2. Install dependencies
go mod tidy

# 3. Build the binary
go build -o dbx-explore
```
### Pre-built Binaries
📦 **[Download Latest Release](https://github.com/Vadoid/dbx-unity-rest-api/releases/latest)**

We automatically build binaries for every release:
- **Linux**: `dbx_uc_Linux_x86_64.tar.gz`
- **macOS (Intel)**: `dbx_uc_Darwin_x86_64.tar.gz`
- **macOS (Apple Silicon)**: `dbx_uc_Darwin_arm64.tar.gz`
- **Windows**: `dbx_uc_Windows_x86_64.zip`


## Usage

### Interactive Mode (Recommended)
The best way to use the tool is the interactive mode, which guides you through everything.

```bash
./dbx-explore interactive
```

**What happens next?**
1. **Login**: If you aren't logged in, it will prompt for your Host and Token (and open your browser to help you create one).
2. **Warehouse Discovery**: It automatically finds a running SQL Warehouse to use for queries.
3. **Exploration**: Select a Catalog, then a Schema, then a Table.
4. **Action**: Choose to View Columns, Metadata, or Sample Data.

### Warehouse Selection
If your default warehouse is stopped or you want to use a Serverless engine:
1. Select **Switch SQL Warehouse** from the Main Menu.
2. Choose from the list (Serverless warehouses are marked with `⚡ [Serverless]`).

### Reset Credentials
If you need to switch workspaces or users:
1. Select **Reset Credentials / Login** from the Main Menu.
2. This clears your `.env` file and starts the login flow again.

## Architecture

- **Language**: Go
- **SDK**: `databricks-sdk-go`
- **Data Access**: 
  - Metadata (Catalogs/Schemas/Tables) via **Unity Catalog REST API**.
  - Data (Rows) via **Statement Execution REST API** (`/api/2.0/sql/statements`).
- **Configuration**: Stores `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_WAREHOUSE_ID` in a local `.env` file.

## Troubleshooting

**"No SQL Warehouses found"**
- Ensure your user has `Can Use` permission on at least one SQL Warehouse.
- Ensure you are in the correct workspace.



**"403 Forbidden"**
- Your token might be expired. Use **Reset Credentials** to generate a new one.


