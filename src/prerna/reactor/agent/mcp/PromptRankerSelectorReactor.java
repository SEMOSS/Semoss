package prerna.reactor.agent.mcp;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.engine.api.IModelEngine;
import prerna.util.Utility;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import org.json.JSONException;

public class PromptRankerSelectorReactor extends AbstractReactor {

    private static final double DEFAULT_THRESHOLD = 0.3;
    private static final String HARDCODED_LLM_ENGINE_ID = "4acbe913-df40-4ac0-b28a-daa5ad91b172";

    public PromptRankerSelectorReactor() {
        this.keysToGet = new String[]{"prompt", "score", "result", "ranker"};
        this.keyRequired = new int[]{1, 1, 1, 0};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String prompt = this.keyValue.get("prompt");
        double score = Double.parseDouble(this.keyValue.get("score"));
        String result = this.keyValue.get("result");
        String ranker = this.keyValue.get("ranker");

        Map<String, Object> info = new HashMap<>();
        info.put("prompt", prompt);
        info.put("score", score);
        info.put("result", result);
        info.put("ranker", ranker);

        // Prepare LLM prompt
        String llmPrompt =
            "You are an automation assistant that evaluates prompt scores and selects the best ranker for rerunning a prompt if needed.\n\n" +
            "Given:\n- prompt: The prompt text.\n- score: The score (double).\n- result: The output from the initial prompt.\n\n" +
            "If the score is below 0.3, select the most suitable ranker from: Semantic, Hybrid, RRF, BM25, based on the prompt and result.\n\n" +
            "Respond ONLY with a JSON object in this format:\n" +
            "{\"rerun\": <true/false>, \"ranker\": \"<ranker_type>\", \"evaluation\": \"<your reasoning>\"}\n\n" +
            "If the score is 0.3 or above, set rerun to false and ranker to null.\n\n" +
            "prompt: \"" + prompt + "\"\n" +
            "score: " + score + "\n" +
            "current ranker method: " + ranker + "\n" +
            "result: \"" + result + "\"";

        IModelEngine llmEng = Utility.getModel(HARDCODED_LLM_ENGINE_ID);
        Map<String, Object> llmParams = new HashMap<>();
        llmParams.put("max_new_tokens", 100);

        Map<String, Object> llmOutput = llmEng.ask(llmPrompt, "", this.insight, llmParams).toMap();
        String llmResponse = (String) llmOutput.get("response");

        // Parse LLM response
        try {
        	String cleanLlmResponse = stripMarkdownJson(llmResponse);
        	JSONObject llmJson = new JSONObject(cleanLlmResponse);
            info.put("rerun", llmJson.optBoolean("rerun", false));
            info.put("ranker_selected", llmJson.optString("ranker", null));
            info.put("evaluation", llmJson.optString("evaluation", ""));
            info.put("llm_response", llmResponse);
        } catch (JSONException e) {
            info.put("rerun", false);
            info.put("ranker_selected", null);
            info.put("evaluation", "Error parsing LLM response: " + e.getMessage());
            info.put("llm_response", llmResponse);
        }

        return new NounMetadata(info, PixelDataType.MAP);
    }
    
    private static String stripMarkdownJson(String response) {
        String trimmed = response.trim();
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
        return "Uses LLM to select the best ranker (Semantic, Hybrid, RRF, BM25) for rerunning a prompt if score is below 0.3. Returns evaluation and selected ranker.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "prompt":
                return "The prompt to be evaluated and possibly reranked.";
            case "score":
                return "The score associated with the prompt (double).";
            case "result":
                return "The result/output from the initial prompt.";
            default:
                return super.getDescriptionForKey(key);
        }
    }
}
