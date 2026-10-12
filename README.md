# dbstuff

## Database utilities

### Prerequisites

- For Python examples, you'll want to create a python virtual environment, activate it, and do `pip install -r requirements.txt`
- For PHP example(s), you'll want to do `composer require mongodb/mongodb`

### Code

- `mapepire_test.py` ... example of databasing via Mapepire on IBM i using the Python driver

- `mariadb_to_mongo.py` .. convert a MySQL/MariaDB table to a MongoDB collection
  - `python mariadb_to_mongo.py -h` for help
- `mongodb-agent` ... a simple example LLM-driven agent for the MongoDB MCP server (**not production ready**)
  - see the README.md in that subdirectory
  - note that the `mongodb-agent` uses its own Python virtual environment if you choose to run the server startup script
- `mongodb_server_status.php` .. simple script demonstrating checking MongoDB server status in PHP

Jack Woehr
2026-10-11
