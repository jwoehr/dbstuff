# MongoDB MCP Agent

This example Node.js application serves as an intelligent agent that leverages a large language model to interact with a [MongoDB MCP server](https://www.mongodb.com/products/tools/mcp-server) instance running as an http server. The agent is designed to understand user questions about MongoDB, select the appropriate tool from the MongoDB MCP server, execute it with the correct arguments, and then format the results into a natural, human-readable answer.

This simple example is **not production ready** for a number of reasons, among which are:

- Unsophisticated prompt
- Security stuff you'd want to do in production
- Assumes the MongoDB MCP server is running in http mode locally
- Agent does not conform to more modern agent superset protocols like [ACP](https://agentclientprotocol.com/overview/introduction) or [A2A](https://a2a-protocol.org/latest/)
- In production agent would possibly run behind a gateway such as [IBM MCP Context Forge](https://github.com/IBM/mcp-context-forge)

... and so on. There is a lot to consider in deploying modern AI technology in production!

## Features

- **Intelligent Tool Selection:** Utilizes Google Gemini or one of several other large language models to analyze user queries and select the most appropriate MongoDB tool from the available tools provided by the MongoDB MCP server.
- **Fallback to Direct Answer:** If the model cannot determine an appropriate tool to use, it will provide a direct answer based on its general knowledge.
- **Dynamic Tool Execution:** Calls the selected tool on the MongoDB MCP server with the necessary arguments.
- **Natural Language Response:** Formats the JSON or structured data returned by the MongoDB tools into a clear and understandable natural language response.
- **Chat History Context:** Considers conversation history to provide more relevant and accurate answers.
- **MCP Session Management:** Maintains a persistent session with the MongoDB MCP server for reliable communication.
- **Interactive Chat Interface:** Includes a command-line chat client for easy interaction with MongoDB.

## Prerequisites

- Node.js (>=18.0.0) and npm installed.
- Access to a running MongoDB MCP server instance.
- An API key for an LLM or Ollama installed locally.

## Configuration

Before running the agent, you need to set up the following environment variables. You can create a `.env` file in the root of this directory to store these variables. An example is provided in `.env.example`.

### Required Environment Variables

- `LLM_PROVIDER`: Choose which LLM provider to use: gemini, ollama, openai, or anthropic
- `GEMINI_API_KEY`: Your API key for the Google Gemini service (or `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` ...).
- `GEMINI_GENERATIVE_MODEL`: The specific Gemini model (or `OPENAI_MODEL` or `ANTHROPIC_MODEL` ... )you want to use (e.g., `gemini-2.0-flash-exp`).
  - See the other factors in the .env.example applicable to your choice of provider and model and fill in appropriately.
- `MONGODB_MCP_SERVER_URI`: The full URI of the running MongoDB MCP server instance (e.g., `http://localhost:3000/mcp`).

### Optional Environment Variables

- `PORT`: The port on which the agent server will listen. Defaults to `3012`.
- `MONGODB_AGENT_URL`: The URL for the chat client to connect to. Defaults to `http://localhost:3012`.

## Installation

1. Navigate to the `mongodb-agent` directory.
2. Copy the example environment file and configure it:

    ```bash
    cp .env.example .env
    # Edit .env with your actual values
    ```

3. Install the required npm packages:

    ```bash
    npm install
    ```

4. If you wish to start the MongoDB MCP server from the provided script `start_mongodb-mcp-server.py`
you may need to `pip install -r requirements.txt`, preferably in a Python virtual environment.

## Running the Agent

### Starting the Agent Server

To start the agent server, run the following command from within the `mongodb-agent` directory:

```bash
npm start
```

Or directly:

```bash
node agent-server.js
```

Upon successful startup, the agent will fetch the available tools from the MongoDB MCP server and will be ready to accept requests on the configured port (default: 3012).

### Using the Interactive Chat Client

To start an interactive chat session with the MongoDB agent:

```bash
npm run chat
```

Or directly:

```bash
node chat.js
```

The chat interface provides:

- Natural language queries about your MongoDB databases
- Command history and context awareness
- Easy-to-use commands (exit, clear, help)

#### Chat Commands

- Type your MongoDB-related questions naturally
- `exit` or `quit` - End the chat session
- `clear` - Clear chat history
- `help` - Display help message

#### Example Queries

```text
🍃 MongoDB Query> List all databases

🍃 MongoDB Query> Show collections in the sample database

🍃 MongoDB Query> Find documents in the users collection

🍃 MongoDB Query> Get performance statistics for my cluster
```

## API Endpoint

### `POST /chat`

This is the main endpoint for interacting with the agent.

**Request Body:**

```json
{
    "question": "Your MongoDB question here",
    "history": []
}
```

- `question` (string, required): The user's question or prompt about MongoDB.
- `history` (array, optional): An array of previous conversation turns to provide context to the model.

**Success Response (200 OK):**

```json
{
    "answer": "The formatted, natural language answer to your MongoDB question."
}
```

**Error Response (500 Internal Server Error):**

```json
{
    "answer": "Sorry, there was an error processing your request."
}
```

## How It Works

1. **Server Initialization:** On startup, the agent establishes a session with the MongoDB MCP server using the MCP protocol initialize handshake.
2. **Tool Discovery:** The agent fetches and caches the list of available MongoDB tools from the MCP server.
3. **User Query:** The user submits a question through the `/chat` endpoint or the interactive chat client.
4. **Tool Selection:** The agent sends the question and chat history to Google Gemini, which analyzes the query and selects the most appropriate MongoDB tool.
5. **Tool Execution:** The agent calls the selected tool on the MongoDB MCP server with the determined arguments using the established session.
6. **Response Formatting:** The raw data returned by the MongoDB MCP server is sent back to Gemini to be formatted into a natural language response.
7. **Result Delivery:** The formatted answer is returned to the user.

## Troubleshooting

- **"Tool list is not available"**: Ensure the MongoDB MCP server is running and accessible at the URI specified in `MONGODB_MCP_SERVER_URI`.
- **Connection Refused**: Verify that the MongoDB MCP server is started and listening on the correct port.
- **Authentication Errors**: Check that your Gemini API key is valid and properly set in the `.env` file.

## Development

The agent consists of two main components:

1. **agent-server.js**: The Express server that handles chat requests and communicates with both Gemini AI and the MongoDB MCP server.
2. **chat.js**: An interactive command-line interface for querying MongoDB using natural language.

## License

Apache-2.0
