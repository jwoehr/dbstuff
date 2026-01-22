/**
 * Google Gemini Provider
 * Implements the BaseLLMProvider interface for Google Gemini
 */
const BaseLLMProvider = require('./base-provider');

class GeminiProvider extends BaseLLMProvider {
    /**
     * Initialize the Gemini provider
     * @param {Object} config - Configuration object
     * @param {string} config.apiKey - Gemini API key
     * @param {string} config.model - Gemini model name
     */
    async initialize(config) {
        if (!config.apiKey) {
            throw new Error('Gemini API key is required');
        }
        if (!config.model) {
            throw new Error('Gemini model name is required');
        }

        try {
            const { GoogleGenerativeAI } = require('@google/generative-ai');
            this.genAI = new GoogleGenerativeAI(config.apiKey);
            this.model = this.genAI.getGenerativeModel({ model: config.model });
            this.modelName = config.model;
        } catch (error) {
            throw new Error(
                'Failed to initialize Gemini provider. ' +
                'Please ensure @google/generative-ai is installed: ' +
                'npm install @google/generative-ai'
            );
        }
    }

    /**
     * Create a new chat session with history
     * @param {Array} history - Conversation history in standardized format
     * @returns {Promise<Object>} Chat session object
     */
    async createChat(history) {
        const geminiHistory = this.transformHistory(history);
        return this.model.startChat({ history: geminiHistory });
    }

    /**
     * Send a message in the chat session
     * @param {Object} chat - Gemini chat session object
     * @param {string} prompt - Message prompt
     * @returns {Promise<Object>} Response object from Gemini
     */
    async sendMessage(chat, prompt) {
        const result = await chat.sendMessage(prompt);
        return result;
    }

    /**
     * Extract text from Gemini response
     * @param {Object} result - Gemini response object
     * @returns {Promise<string>} Extracted text
     */
    async extractTextResponse(result) {
        const response = await result.response;
        return response.text();
    }

    /**
     * Transform standardized history to Gemini format
     * @param {Array} history - Standardized history format
     * @returns {Array} Gemini-specific history format
     */
    transformHistory(history) {
        if (!history || history.length === 0) {
            return [];
        }

        return history.map(entry => {
            // If already in Gemini format, return as-is
            if (entry.parts) {
                return entry;
            }

            // Convert from standardized format
            return {
                role: entry.role === 'assistant' ? 'model' : entry.role,
                parts: [{ text: entry.content || '' }]
            };
        });
    }

    /**
     * Get provider metadata
     * @returns {Object} Provider information
     */
    getMetadata() {
        return {
            name: 'Google Gemini',
            provider: 'gemini',
            model: this.modelName,
            version: '1.0.0'
        };
    }
}

module.exports = GeminiProvider;
