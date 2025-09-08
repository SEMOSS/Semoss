package prerna.reactor.database.metaeditor.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IDatabaseEngine;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class PredictOwlDescriptionLLMReactor extends AbstractReactor {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = LogManager.getLogger(PredictOwlDescriptionLLMReactor.class);

    public PredictOwlDescriptionLLMReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.DATABASE.getKey(),
                ReactorKeysEnum.CONCEPT.getKey(),
                ReactorKeysEnum.COLUMN.getKey(),
                ReactorKeysEnum.ENGINE.getKey()
        };
        this.keyRequired = new int[] { 1, 1, 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
        String concept = this.keyValue.get(ReactorKeysEnum.CONCEPT.getKey());
        String column = this.keyValue.get(ReactorKeysEnum.COLUMN.getKey());
        String llmEngineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

        try {
            IDatabaseEngine database = Utility.getDatabase(databaseId);

            String qsName = concept + "__" + column;
            String physicalUri = database.getPhysicalUriFromPixelSelector(qsName);
            SemossDataType dataType = SemossDataType.convertStringToDataType(database.getDataTypes(physicalUri));

            Set<String> logicalNames = database.getLogicalNames(physicalUri);

            List<String> sampleValues = new ArrayList<>();
            sampleValues.add(concept);
            sampleValues.add(column);
            sampleValues.addAll(logicalNames);

            if (dataType == SemossDataType.STRING) {
                log.info("Grabbing sample values for column: " + column);
                IRawSelectWrapper wrapper = null;
                try {
                    wrapper = WrapperManager.getInstance().getRawWrapper(database,
                            getMostOccuringSingleColumnNonEmptyQs(qsName, 10));
                    while (wrapper.hasNext()) {
                        Object value = wrapper.next().getValues()[0];
                        if (value == null || value.toString().isEmpty()) {
                            continue;
                        }
                        sampleValues.add(value.toString());
                    }
                } catch (Exception e) {
                    log.error("Error getting sample values", e);
                } finally {
                    if (wrapper != null) {
                        try {
                            wrapper.close();
                        } catch (IOException e) {
                            log.error("Error closing wrapper", e);
                        }
                    }
                }
            }

            String prompt = buildPrompt(concept, column, sampleValues);

            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("temperature", 0.3);
            paramMap.put("max_completion_tokens", 1000);

            IModelEngine modelEngine = Utility.getModel(llmEngineId);
            @SuppressWarnings("unchecked")
            Map<String, Object> response = modelEngine.ask(
                    prompt,
                    null,
                    this.insight,
                    paramMap).toMap();

            Object rawResponse = response.get("response");
            String description = "";

            if (rawResponse instanceof String) {
                String jsonStr = ((String) rawResponse).trim();
                if (jsonStr.startsWith("{")) {
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> parsed = mapper.readValue(jsonStr, Map.class);
                        description = (String) parsed.get("description");
                        if (description == null) {
                            description = jsonStr;
                        }
                    } catch (Exception jsonParseException) {
                        log.warn("Failed to parse JSON response, using raw string: " + jsonParseException.getMessage());
                        description = jsonStr;
                    }
                } else {
                    description = jsonStr;
                }
            } else if (rawResponse instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> parsed = (Map<String, Object>) rawResponse;
                description = (String) parsed.get("description");
                if (description == null) {
                    description = rawResponse.toString();
                }
            }

            if (description == null || description.isEmpty()) {
                log.warn("No description generated by LLM, rawResponse was: " + rawResponse);
                throw new RuntimeException("No description generated by LLM");
            }

            description = cleanDescription(description);

            NounMetadata noun = new NounMetadata(new String[] { description }, PixelDataType.CONST_STRING);
            noun.addAdditionalReturn(new NounMetadata("Successfully predicted description",
                    PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
            return noun;

        } catch (Exception e) {
            log.error("Failed to predict description", e);
            NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
            noun.addAdditionalReturn(new NounMetadata("Error predicting description: " + e.getMessage(),
                    PixelDataType.CONST_STRING, PixelOperationType.ERROR));
            return noun;
        }
    }

    private String buildPrompt(String concept, String column, List<String> sampleValues) {
        StringBuilder prompt = new StringBuilder();
        prompt.append("You are a database documentation assistant.\n");
        prompt.append("I need you to generate a clear, concise description for a database column.\n\n");
        prompt.append("Table: ").append(concept).append("\n");
        prompt.append("Column: ").append(column).append("\n");

        if (!sampleValues.isEmpty()) {
            prompt.append("Sample values: ")
                    .append(String.join(", ", sampleValues.subList(0, Math.min(10, sampleValues.size())))).append("\n");
        }

        prompt.append("\nPlease provide a JSON response with the following EXACT format:\n");
        prompt.append("{\n");
        prompt.append("  \"description\": \"<clear, concise description of what this column represents>\"\n");
        prompt.append("}\n\n");
        prompt.append("IMPORTANT RULES:\n");
        prompt.append("- The description should be 1-2 sentences maximum\n");
        prompt.append("- Focus on what the column represents, not technical details\n");
        prompt.append("- Use clear, business-friendly language\n");
        prompt.append("- Return ONLY valid JSON with proper escaping for quotes\n");
        prompt.append("- Do NOT include any text before or after the JSON\n");
        prompt.append("- Do NOT use backslashes unless properly escaped\n");

        return prompt.toString();
    }

    private String cleanDescription(String description) {
        if (description == null) {
            return "";
        }

        description = description.trim();

        if (description.startsWith("\"") && description.endsWith("\"")) {
            description = description.substring(1, description.length() - 1);
        }

        description = description.replace("\\\"", "\"")
                .replace("\\n", " ")
                .replace("\\r", " ")
                .replace("\\t", " ")
                .replaceAll("\\s+", " ")
                .trim();

        return description;
    }

    private String getMostOccuringSingleColumnNonEmptyQs(String qsName, int limit) {
        return "Database(database=[\"" + this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()) + "\"]) | " +
                "Select(" + qsName + ") | " +
                "Where(" + qsName + " != \"\" AND " + qsName + " IS NOT NULL) | " +
                "GroupBy(" + qsName + ") | " +
                "Count() | " +
                "OrderBy(Count DESC) | " +
                "Collect(" + limit + ")";
    }
}
