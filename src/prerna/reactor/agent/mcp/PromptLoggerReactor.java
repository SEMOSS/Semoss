package prerna.reactor.agent.mcp;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.HashMap;
import java.util.Map;

public class PromptLoggerReactor extends AbstractReactor {

    private static final double DEFAULT_THRESHOLD = 0.3;

    public PromptLoggerReactor() {
        this.keysToGet = new String[]{"prompt", "score"};
        this.keyRequired = new int[]{1, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String prompt = this.keyValue.get("prompt");
        double score = Double.parseDouble(this.keyValue.get("score"));

        Map<String, Object> logInfo = new HashMap<>();
        logInfo.put("prompt", prompt);
        logInfo.put("score", score);

        if (score > DEFAULT_THRESHOLD) {
            // Log the information (replace with your logging mechanism if needed)
            System.out.println("Logging prompt: " + prompt + " | Score: " + score);
            logInfo.put("logged", true);
            logInfo.put("evaluation", "Prompt score above threshold, logged for review.");
        } else {
            logInfo.put("logged", false);
            logInfo.put("evaluation", "Prompt score below threshold, not logged.");
        }

        return new NounMetadata(logInfo, PixelDataType.MAP);
    }

    @Override
    public String getReactorDescription() {
        return "Logs prompt and score if score is above 0.3, returns evaluation summary.";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if ("prompt".equals(key)) {
            return "The prompt to be logged and evaluated.";
        } else if ("score".equals(key)) {
            return "The score associated with the prompt (double).";
        }
        return super.getDescriptionForKey(key);
    }
}
