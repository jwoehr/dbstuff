# dbstuff

Database utilities

* `mariadb_to_mongo.py` .. convert a MySQL/MariaDB table to a MongoDB collection.
  1. Create a Python virtual environment in the top directory
  1. `pip install -r requirements.txt`
  1. `python mariadb_to_mongo.py -h` for help.
* `mongodb-agent` ... a simple example LLM-driven agent for the MongoDB MCP server (**not production ready**)
  * see the README.md in that subdirectory
  * note that the `mongodb-agent` uses its own Python virtual environment if you choose to run the server startup script

Jack Woehr
2026-01-22
