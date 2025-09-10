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
                ReactorKeysEnum.ENGINE.getKey(),
                "useSampleValues"
        };
        this.keyRequired = new int[] { 1, 1, 1, 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String databaseId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
        String concept = this.keyValue.get(ReactorKeysEnum.CONCEPT.getKey());
        String column = this.keyValue.get(ReactorKeysEnum.COLUMN.getKey());
        String llmEngineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
        String useSampleValuesStr = this.keyValue.get("useSampleValues");
        boolean useSampleValues = useSampleValuesStr != null && "true".equalsIgnoreCase(useSampleValuesStr);

        try {
            IDatabaseEngine database = Utility.getDatabase(databaseId);

            String qsName = concept + "__" + column;
            String physicalUri = database.getPhysicalUriFromPixelSelector(qsName);
            SemossDataType dataType = SemossDataType.convertStringToDataType(database.getDataTypes(physicalUri));

            Set<String> logicalNames = database.getLogicalNames(physicalUri);

            String physicalTableUri = database.getPhysicalUriFromPixelSelector(concept);
            String physicalTableName = Utility.getInstanceName(physicalTableUri);
            String physicalColumnName = Utility.getClassName(physicalUri);

            List<String> sampleValues = new ArrayList<>();
            sampleValues.add(concept);
            sampleValues.add(column);
            sampleValues.addAll(logicalNames);

            if (useSampleValues) {
                log.info("Grabbing sample values for column: " + column);
                log.info("Database ID: " + databaseId);
                log.info("Concept: " + concept);
                log.info("Column: " + column);
                log.info("QS Name: " + qsName);
                log.info("Physical URI: " + physicalUri);
                log.info("Physical Table Name: " + physicalTableName);
                log.info("Physical Column Name: " + physicalColumnName);
                log.info("Data Type: " + dataType);

                IRawSelectWrapper wrapper = null;
                try {
                    String query = getMostOccuringSingleColumnNonEmptyQs(physicalTableName, physicalColumnName,
                            dataType, 10);
                    log.info("Generated query for sample values: " + query);

                    wrapper = WrapperManager.getInstance().getRawWrapper(database, query);
                    while (wrapper.hasNext()) {
                        Object value = wrapper.next().getValues()[0];
                        if (value == null || value.toString().isEmpty()) {
                            continue;
                        }
                        sampleValues.add(value.toString());
                    }
                    log.info("Retrieved " + (sampleValues.size() - 3) + " sample values");
                    if (sampleValues.size() > 3) {
                        List<String> actualSampleValues = sampleValues.subList(3, sampleValues.size());
                        log.info("Sample values obtained: " + actualSampleValues);
                    } else {
                        log.info("No actual sample values obtained (only concept/column/logical names)");
                    }
                } catch (Exception e) {
                    log.error(
                            "Error getting sample values for concept: " + concept + ", column: " + column
                                    + ", physical table: " + physicalTableName + ", physical column: "
                                    + physicalColumnName + ", query: "
                                    + getMostOccuringSingleColumnNonEmptyQs(physicalTableName, physicalColumnName,
                                            dataType, 10),
                            e);
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

            String prompt = buildPrompt(concept, column, sampleValues, useSampleValues, dataType);

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
            log.info("LLM raw response: " + rawResponse);
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

    private String buildPrompt(String concept, String column, List<String> sampleValues, boolean useSampleValues,
            SemossDataType dataType) {
        StringBuilder prompt = new StringBuilder();
        prompt.append("You are a database documentation assistant.\n");
        prompt.append("I need you to generate a clear, concise description for a database column.\n\n");
        prompt.append("Table: ").append(concept).append("\n");
        prompt.append("Column: ").append(column).append("\n");
        prompt.append("Data Type: ").append(dataType != null ? dataType.toString() : "UNKNOWN").append("\n");

        if (useSampleValues && !sampleValues.isEmpty() && sampleValues.size() > 3) {
            List<String> actualSampleValues = sampleValues.subList(3, sampleValues.size());
            if (!actualSampleValues.isEmpty()) {
                prompt.append("Sample values: ")
                        .append(String.join(", ",
                                actualSampleValues.subList(0, Math.min(10, actualSampleValues.size()))))
                        .append("\n");
            }
        }

        prompt.append("\nPlease provide a JSON response with the following EXACT format:\n");
        prompt.append("{\n");
        prompt.append("  \"description\": \"<clear, concise description of what this column represents>\"\n");
        prompt.append("}\n\n");
        prompt.append("IMPORTANT RULES:\n");
        prompt.append("- The description should be 1-2 sentences maximum\n");
        prompt.append("- Focus on what the column represents, not technical details\n");
        prompt.append("- Use clear, business-friendly language\n");
        prompt.append("- Consider the data type when describing the column's purpose\n");
        prompt.append("- For numeric columns, consider if they represent counts, measurements, IDs, etc.\n");
        prompt.append("- For date/time columns, consider what event or time period they represent\n");
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

    private String getMostOccuringSingleColumnNonEmptyQs(String physicalTableName, String physicalColumnName,
            SemossDataType dataType, int limit) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(physicalColumnName).append(", COUNT(*) as count_val ")
                .append("FROM ").append(physicalTableName).append(" ")
                .append("WHERE ").append(physicalColumnName).append(" IS NOT NULL");

        if (dataType == SemossDataType.STRING) {
            query.append(" AND ").append(physicalColumnName).append(" != ''");
        }

        query.append(" GROUP BY ").append(physicalColumnName).append(" ")
                .append("ORDER BY count_val DESC ")
                .append("LIMIT ").append(limit);

        return query.toString();
    }
}
