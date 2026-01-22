const express = require('express');
const cors = require('cors');
const ProviderFactory = require('./providers/provider-factory');
const ProviderConfig = require('./config/provider-config');
require('dotenv').config();

// Validate required environment variables and get provider config
let providerName, providerConfig;
try {
    const config = ProviderConfig.validate();
    providerName = config.provider;
    providerConfig = config.config;
} catch (error) {
    console.error('Configuration Error:', error.message);
    console.error('\nPlease check your .env file and ensure all required variables are set.');
    console.error('See .env.example for reference.');
    process.exit(1);
}

const app = express();
const port = process.env.PORT || 3012;

app.use(express.json());
app.use(cors());

// --- LLM Provider Setup (abstracted) ---
let llmProvider;

// --- MongoDB MCP Server setup ---
const MONGODB_MCP_SERVER_URI = process.env.MONGODB_MCP_SERVER_URI;

// Validate MongoDB MCP server URI
if (!MONGODB_MCP_SERVER_URI) {
    console.error('Missing required environment variable: MONGODB_MCP_SERVER_URI');
    process.exit(1);
}

let sessionId = null;

let availableTools = [];
let formattedToolList = '';

// --- Utility Functions ---

/**
 * Parses SSE-formatted response data line
 * @param {string} rawResponse - Raw response text with SSE format
 * @returns {Object} Parsed JSON object
 * @throws {Error} If parsing fails
 */
const parseSSEResponse = (rawResponse) => {
    // Extract the data line from SSE format (data: {...})
    const lines = rawResponse.split('\n');
    const dataLine = lines.find(line => line.startsWith('data:'));
    if (!dataLine) {
        throw new Error('No data line found in response');
    }
    const jsonString = dataLine.substring(5).trim(); // Remove "data:" prefix
    try {
        return JSON.parse(jsonString);
    } catch (error) {
        throw new Error(`Failed to parse SSE response: ${error.message}`);
    }
};

/**
 * Initializes a session with the MongoDB MCP server
 * @returns {Promise<string>} Session ID
 * @throws {Error} If initialization fails
 */
const initializeSession = async () => {
    try {
        const response = await fetch(MONGODB_MCP_SERVER_URI, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/event-stream',
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: 1,
                method: 'initialize',
                params: {
                    protocolVersion: '2024-11-05',
                    capabilities: {
                        tools: {},
                        resources: {},
                        prompts: {}
                    },
                    clientInfo: {
                        name: 'mongodb-agent',
                        version: '1.0.0'
                    }
                }
            }),
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        // Extract session ID from response headers
        const sessionIdHeader = response.headers.get('mcp-session-id');
        if (!sessionIdHeader) {
            throw new Error('No mcp-session-id header in response');
        }

        // Send notifications/initialized
        await fetch(MONGODB_MCP_SERVER_URI, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/event-stream',
                'Mcp-Session-Id': sessionIdHeader,
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                method: 'notifications/initialized'
            }),
        });

        console.log('✓ Successfully initialized session with MongoDB MCP server');
        return sessionIdHeader;
    } catch (error) {
        console.error('Failed to initialize session:', error);
        throw error;
    }
};

/**
 * Sends a direct answer from the LLM without using tools
 * @param {Object} chat - LLM chat instance
 * @param {string} question - User's question
 * @param {Object} res - Express response object
 * @returns {Promise<void>}
 */
const sendDirectAnswer = async (chat, question, res) => {
    const result = await llmProvider.sendMessage(chat, question);
    const finalAnswer = await llmProvider.extractTextResponse(result);
    return res.json({ answer: finalAnswer });
};

/**
 * Creates the tool selection prompt
 * @param {string} question - User's question
 * @param {string} formattedTools - Formatted tool list JSON string
 * @returns {string} Tool selection prompt
 */
const createToolSelectionPrompt = (question, formattedTools) => `
    You are an expert at selecting the correct tool to answer a user's question about MongoDB.
    Here is a list of available tools:
    ${formattedTools}

    Here is the user's question: "${question}"

    Based on the user's question and the chat history, which tool should be used?
    Also, what are the arguments for the tool? If there are no arguments, please provide an empty object.
    Your response should be a JSON object with two keys: "toolName" and "toolArguments".
`;

/**
 * Creates the answer formatting prompt
 * @param {string} question - User's question
 * @param {Object} resultData - Tool result data
 * @returns {string} Answer formatting prompt
 */
const createAnswerFormattingPrompt = (question, resultData) => `
    You are an expert at formatting MongoDB technical data into a clear, human-readable answer.
    The user asked the following question: "${question}"
    The following data was returned from a tool:
    ${JSON.stringify(resultData, null, 2)}

    Please format this data into a natural language answer that directly addresses the user's question.
    Do not just repeat the JSON. Explain what the data means.
`;

/**
 * Selects appropriate tool based on user question
 * @param {Object} chat - LLM chat instance
 * @param {string} question - User's question
 * @param {string} formattedTools - Formatted tool list JSON string
 * @returns {Promise<Object|null>} Tool selection object or null if fallback needed
 */
const selectTool = async (chat, question, formattedTools) => {
    const prompt = createToolSelectionPrompt(question, formattedTools);
    const result = await llmProvider.sendMessage(chat, prompt);
    const textResponse = await llmProvider.extractTextResponse(result);
    const match = textResponse.match(/```json\n([\s\S]*?)```/);
    const jsonString = match ? match[1] : textResponse;
    
    try {
        return JSON.parse(jsonString);
    } catch (error) {
        console.warn('Tool selection JSON parsing failed, falling back to direct answer:', error.message);
        return null;
    }
};

/**
 * Calls an MCP tool and returns the result with automatic session recovery
 * @param {string} toolName - Name of the tool to call
 * @param {Object} toolArguments - Arguments for the tool
 * @param {boolean} isRetry - Whether this is a retry after session reconnection
 * @returns {Promise<Object>} Tool result data
 * @throws {Error} If tool call fails or returns invalid response
 */
const callMCPTool = async (toolName, toolArguments, isRetry = false) => {
    try {
        const response = await fetch(MONGODB_MCP_SERVER_URI, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/event-stream',
                'Mcp-Session-Id': sessionId,
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: Date.now(),
                method: 'tools/call',
                params: {
                    name: toolName,
                    arguments: toolArguments,
                },
            }),
        });
        
        // Check for session errors
        if (response.status === 401 || response.status === 403 || response.status === 404) {
            if (!isRetry) {
                console.log('Session may have expired, attempting to reconnect...');
                sessionId = await initializeSession();
                await fetchTools();
                console.log('Reconnected, retrying tool call...');
                return await callMCPTool(toolName, toolArguments, true);
            } else {
                throw new Error('Session reconnection failed');
            }
        }
        
        const rawResponse = await response.text();
        const toolData = parseSSEResponse(rawResponse);
        
        if (!toolData.result) {
            throw new Error('Invalid response from tool server');
        }
        
        return toolData.result.structuredContent?.data
            || toolData.result.content
            || toolData.result;
    } catch (error) {
        // If it's a connection error and not already retrying, try to reconnect
        if (!isRetry && (error.code === 'ECONNREFUSED' || error.message.includes('fetch failed'))) {
            console.log('Connection lost, attempting to reconnect...');
            try {
                sessionId = await initializeSession();
                await fetchTools();
                console.log('Reconnected successfully, retrying tool call...');
                return await callMCPTool(toolName, toolArguments, true);
            } catch (reconnectError) {
                console.error('Reconnection failed:', reconnectError);
                throw new Error('Lost connection to MongoDB MCP server and reconnection failed');
            }
        }
        throw error;
    }
};

// --- Tool Fetching ---
/**
 * Fetches available tools from the MongoDB MCP server
 * @returns {Promise<void>}
 * @throws {Error} If the server is unreachable or returns invalid data
 */
const fetchTools = async () => {
    try {
        const response = await fetch(MONGODB_MCP_SERVER_URI, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json, text/event-stream',
                'Mcp-Session-Id': sessionId,
            },
            body: JSON.stringify({
                jsonrpc: '2.0',
                id: 2,
                method: 'tools/list',
                params: {},
            }),
        });
        const rawResponse = await response.text();
        const data = parseSSEResponse(rawResponse);
        availableTools = data.result.tools.map(tool => ({
            name: tool.name,
            description: tool.description,
            inputSchema: tool.inputSchema,
        }));
        formattedToolList = JSON.stringify(availableTools, null, 2);
        console.log(`✓ Successfully fetched ${availableTools.length} tools from mongodb-mcp-server.`);
    } catch (error) {
        console.error('FATAL: Failed to fetch tools from mongodb-mcp-server:', error);
        process.exit(1);
    }
};

// --- Chat Endpoint ---
/**
 * Handles chat requests by selecting and calling appropriate tools from the MongoDB MCP server
 * @param {Object} req - Express request object
 * @param {string} req.body.question - The user's question
 * @param {Array} req.body.history - Chat history for context
 * @param {Object} res - Express response object
 * @returns {Promise<void>} Sends JSON response with answer or error
 */
app.post('/chat', async (req, res) => {
    const { question, history } = req.body;

    if (availableTools.length === 0) {
        return res.status(500).json({
            answer: 'Tool list is not available. Please check the connection to the mongodb-mcp-server.'
        });
    }

    try {
        // Standardize history format if needed
        const standardizedHistory = llmProvider.standardizeHistory(history || []);
        
        // Create chat with provider
        const chat = await llmProvider.createChat(standardizedHistory);

        // Step 1: Select appropriate tool
        const toolSelection = await selectTool(chat, question, formattedToolList);
        if (!toolSelection?.toolName) {
            console.log('No specific tool selected, providing direct answer');
            return await sendDirectAnswer(chat, question, res);
        }

        console.log(`Tool selected: ${toolSelection.toolName}`);
        console.log(`Tool arguments: ${JSON.stringify(toolSelection.toolArguments, null, 2)}`);

        // Step 2: Call the selected tool
        const resultData = await callMCPTool(toolSelection.toolName, toolSelection.toolArguments);

        // Step 3: Format the answer using the LLM
        const prompt = createAnswerFormattingPrompt(question, resultData);
        const result = await llmProvider.sendMessage(chat, prompt);
        const finalAnswer = await llmProvider.extractTextResponse(result);

        res.json({ answer: finalAnswer });

    } catch (error) {
        console.error('Error in LLM agent:', error);
        res.status(500).json({ answer: 'Sorry, there was an error processing your request.' });
    }
});

// --- Server Startup ---
app.listen(port, async () => {
    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║          MongoDB MCP Agent Server                        ║');
    console.log('╚══════════════════════════════════════════════════════════╝');
    console.log(`\nServer starting at http://localhost:${port}`);
    console.log(`LLM Provider: ${providerName}`);
    
    try {
        // Initialize LLM provider
        llmProvider = await ProviderFactory.createProvider(providerName, providerConfig);
        const metadata = llmProvider.getMetadata();
        console.log(`Model: ${metadata.model || 'unknown'}`);
        
        // Initialize MongoDB MCP session
        sessionId = await initializeSession();
        
        // Fetch tools from MCP server
        await fetchTools();
        
        console.log(`\n✓ MongoDB MCP Agent Server ready at http://localhost:${port}\n`);
    } catch (error) {
        console.error('\nFATAL: Failed to start server:', error.message);
        process.exit(1);
    }
});
