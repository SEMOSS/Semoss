package prerna.reactor.agent.mcp;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.json.JSONException;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.util.Utility;

public class SimpleLLMCallReactor extends AbstractReactor {

    private static final String HARDCODED_LLM_ENGINE_ID = "4acbe913-df40-4ac0-b28a-daa5ad91b172";

    public SimpleLLMCallReactor() {
        this.keysToGet = new String[] {"prompt", "project", "score", "result", "ranker"};
        this.keyRequired = new int[] {1, 1, 0, 0, 0}; // score/result optional, only needed for PromptRankerSelector
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String userPrompt = this.keyValue.get("prompt");
        String projectId = this.keyValue.get("project");
        String score = this.keyValue.get("score");
        String result = this.keyValue.get("result");
        String ranker = this.keyValue.get("ranker");

        if (userPrompt == null || userPrompt.isEmpty()) {
            throw new IllegalArgumentException("Must provide a valid prompt");
        }
        if (projectId == null || projectId.isEmpty()) {
            throw new IllegalArgumentException("Must provide a valid project ID");
        }

        // Prepare NounStore for GetMCPToolsReactor
        NounStore toolsNs = new NounStore("ALL");
        toolsNs.makeNoun("project").add(projectId, PixelDataType.CONST_STRING);

        // Execute GetMCPToolsReactor
        GetMCPToolsReactor toolsReactor = new GetMCPToolsReactor();
        toolsReactor.setInsight(this.insight);
        toolsReactor.setNounStore(toolsNs);
        toolsReactor.In(); // Optional

        NounMetadata toolsResult = toolsReactor.execute();
        JSONObject toolsJson = (JSONObject) toolsResult.getValue();

        // Prepare context for LLM
        String toolsStr = toolsJson.toString(2);

        // LLM prompt: pass in the actual toolsStr
        String llmPrompt =
            "You are an automation assistant that receives user messages and decides which tool to invoke from a list of available tools described in the JSON below.\n" +
            "For each user message, respond ONLY with a JSON object specifying the tool to invoke and its arguments, in the following format:\n" +
            "{\"tool\": \"<tool_name>\", \"arguments\": {\"<arg1>\": \"<value1>\", \"<arg2>\": \"<value2>\"}}\n\n" +
            "Available tools (JSON):\n" +
            toolsStr + "\n\n" +
            "Do not include any explanation, only the JSON object.\n\n" +
            "User message: " + userPrompt +
            (score != null ? ("\nScore: " + score) : "") +
            (ranker != null ? ("\nRanker Method: " + ranker) : "") +
            (result != null ? ("\nResult: " + result) : "");

        // Get the LLM engine using the hardcoded ID
        IModelEngine llmEng = Utility.getModel(HARDCODED_LLM_ENGINE_ID);

        Map<String, Object> llmParams = new HashMap<>();
        llmParams.put("max_new_tokens", 300);

        // Call LLM and get output
        Map<String, Object> llmOutput = llmEng.ask(llmPrompt, "", this.insight, llmParams).toMap();
        String llmResponse = (String) llmOutput.get("response");

        // Clean the LLM response before parsing as JSON
        String cleanLlmResponse = stripMarkdownJson(llmResponse);

        // Parse the LLM response
        JSONObject llmJson = null;
        NounMetadata toolResult = null;
        try {
            llmJson = new JSONObject(cleanLlmResponse);
            String toolName = llmJson.getString("tool");
            JSONObject arguments = llmJson.getJSONObject("arguments");

            if ("PromptLogger".equalsIgnoreCase(toolName)) {
                // Prepare NounStore for PromptLoggerReactor
                NounStore loggerNs = new NounStore("ALL");
                loggerNs.makeNoun("score").add(arguments.getString("score"), PixelDataType.CONST_STRING);
                loggerNs.makeNoun("prompt").add(arguments.getString("prompt"), PixelDataType.CONST_STRING);

                PromptLoggerReactor loggerReactor = new PromptLoggerReactor();
                loggerReactor.setInsight(this.insight);
                loggerReactor.setNounStore(loggerNs);

                toolResult = loggerReactor.execute();

            } else if ("PromptRankerSelector".equalsIgnoreCase(toolName)) {
                // Prepare NounStore for PromptRankerSelectorReactor
                NounStore rankerNs = new NounStore("ALL");
                rankerNs.makeNoun("score").add(arguments.getString("score"), PixelDataType.CONST_STRING);
                rankerNs.makeNoun("prompt").add(arguments.getString("prompt"), PixelDataType.CONST_STRING);
                rankerNs.makeNoun("result").add(arguments.optString("result", ""), PixelDataType.CONST_STRING);
                rankerNs.makeNoun("ranker").add(arguments.optString("ranker"), PixelDataType.CONST_STRING);

                PromptRankerSelectorReactor rankerReactor = new PromptRankerSelectorReactor();
                rankerReactor.setInsight(this.insight);
                rankerReactor.setNounStore(rankerNs);

                toolResult = rankerReactor.execute();
            }
            // Add more tool mappings here as needed

        } catch (JSONException e) {
            // Handle parsing errors
            System.err.println("Error parsing LLM response: " + e.getMessage());
        }

        // Output both the LLM response and the tool execution result
        Map<String, Object> output = new HashMap<>();
//        output.put("prompt", userPrompt);
//        output.put("project", projectId);
//        output.put("tools", toolsJson);
        output.put("llm_response", llmResponse);
        output.put("tool_result", toolResult != null ? toolResult.getValue() : null);

        return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
    }

    /**
     * Utility to strip markdown code fencing and language tags from LLM response.
     */
    private static String stripMarkdownJson(String response) {
        String trimmed = response.trim();
        // Remove triple backticks and optional language tag
        if (trimmed.startsWith("```")) {
            int firstNewline = trimmed.indexOf('\n');
            if (firstNewline != -1) {
                trimmed = trimmed.substring(firstNewline + 1);
            }
            if (trimmed.endsWith("```")) {
                trimmed = trimmed.substring(0, trimmed.length() - 3);
            }
            trimmed = trimmed.trim();
        }
        return trimmed;
    }

    @Override
    public String getReactorDescription() {
        return "Calls GetMCPToolsReactor for a project, passes the result as context to an LLM call, parses the LLM output, and triggers the selected tool, returning both LLM output and tool result.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if ("prompt".equals(key)) {
            return "The prompt to send to the LLM.";
        } else if ("project".equals(key)) {
            return "The project ID to retrieve MCP tools.";
        } else if ("score".equals(key)) {
            return "The score associated with the prompt (optional, for ranker selection).";
        } else if ("result".equals(key)) {
            return "The result/output from the initial prompt (optional, for ranker selection).";
        }
        return super.getDescriptionForKey(key);
    }
}
