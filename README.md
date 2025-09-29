# ETL Pipeline Builder

A graphical ETL (Extract, Transform, Load) application built with Python and Polars.

## Features

- Visual pipeline builder interface
- Support for multiple data sources and destinations
- Data transformation capabilities
- Real-time pipeline visualization
- Efficient data processing with Polars

### Supported Sources

- CSV files
- Excel files (read via Polars fallback to pandas)
- JSON files
- Parquet files
- Databases (MySQL, PostgreSQL, SQL Server, SQLite)
- HTTP APIs (GET/POST/etc.)

### Supported Destinations

- CSV files
- Excel files (write via pandas/openpyxl)
- JSON files
- Parquet files
- Databases (MySQL, PostgreSQL, SQL Server, SQLite)
- HTTP APIs (JSON batch sending)

## Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the main application:
```bash
python main.py
```

### Building a Pipeline

1. Use the left palette to add Source, Transform, and Destination nodes.
2. Drag from a source's output to a transform/destination input to connect.
3. Select a node to configure it in the Properties panel on the right.
4. For file-based sources/destinations, choose the file path and (optionally) format.
5. For DB sources/destinations, fill connection info and query/table.
6. For API sources/destinations, set URL, method, headers and params (e.g. `k1:v1,k2:v2`).
7. Use the Run menu to Execute Pipeline or Stop Pipeline.

### Save / Load Pipeline

- File -> Guardar Pipeline: saves the current graph (nodes, positions, connections, and configs) to a `.etl.json` file.
- File -> Abrir Pipeline: loads a previously saved `.etl.json` and rebuilds nodes and connections.

Notes:
- DataFrames are not embedded in the saved file (only configurations), keeping files small.
- When loading, node positions and configs are restored. You can re-run to refresh data.

### Stop Execution

- Run -> Detener Pipeline sends a stop request. Long operations (DB/API/large files) will stop as soon as safely possible.
## Project Structure

- `main.py`: Main application entry point
- `gui/`: GUI components and interface
- `core/`: Core ETL functionality
- `utils/`: Utility functions and helpers 

## Dependencies
Install dependencies:
```bash
pip install -r requirements.txt
```

Additional notes:
- Excel: writing uses pandas + openpyxl.
- Databases: uses SQLAlchemy. Install the appropriate driver:
  - PostgreSQL: `psycopg2-binary`
  - MySQL: `PyMySQL`
  - SQL Server: `pyodbc` (system ODBC driver required)
  - SQLite: included with Python
  - Faster DB reads try `connectorx` when available.