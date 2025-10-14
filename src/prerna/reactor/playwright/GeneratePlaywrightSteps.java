package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GeneratePlaywrightStepsReactor extends AbstractReactor {
    
    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    
    public GeneratePlaywrightStepsReactor() {
        this.keysToGet = new String[] {
            "engine",
            "sessionId",
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String engineId = this.keyValue.get(this.keysToGet[0]);
        String sessionId = this.keyValue.get(this.keysToGet[1]);
        Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);
        
        Map<String, Object> result = generateSteps(engineId, sessionId, paramValues);
        return new NounMetadata(result, PixelDataType.MAP);
    }

    private Map<String, Object> generateSteps(String engineId, String sessionId, Map<String, Object> params) {
        try {
            // Get the HTML extraction data
            @SuppressWarnings("unchecked")
            Map<String, Object> extractionData = (Map<String, Object>) params.get("extractionData");
            
            if (extractionData == null) {
                throw new IllegalArgumentException("extractionData is required");
            }

            String userContext = (String) params.getOrDefault("userContext", " ");
            if(userContext == null || userContext.trim().isEmpty()) {
                userContext = " ";
            }

            //cropParams
            Map<String, Object> cropParams = (Map<String, Object>) params.get("cropParams");
            ScreenshotResponse croppedImage = ScreenshotReactor.croppedScreenshot(
                sessionId,
                ((Number) cropParams.get("startX")).intValue(),
                ((Number) cropParams.get("startY")).intValue(),
                ((Number) cropParams.get("endX")).intValue(),
                ((Number) cropParams.get("endY")).intValue()
            );

            // Get interactive elements only
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> allElements = (List<Map<String, Object>>) extractionData.get("elements");
            List<Map<String, Object>> interactiveElements = allElements.stream()
                .filter(e -> Boolean.TRUE.equals(e.get("interactive")))
                .toList();
            
            String prompt = buildPrompt(extractionData, interactiveElements, userContext);

            System.out.println("Generated Prompt: " + prompt); // For debugging
            
            IModelEngine modelEngine = Utility.getModel(engineId);

            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("image_encoded", croppedImage.base64Png());
            
            Map<String, Object> modelOutput = modelEngine.ask(str(prompt), null, this.insight, paramMap).toMap();
            
            String aiResponse = (String) modelOutput.get("response");
            
            // Try to extract JSON array from the response
            String cleanedResponse = extractJsonArray(aiResponse);
            
            Map<String, Object> result = new HashMap<>();
            result.put("rawResponse", aiResponse);
            result.put("stepsJson", cleanedResponse);
            result.put("success", true);
            
            return result;
            
        } catch (Exception e) {
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("success", false);
            errorResult.put("error", e.getMessage());
            errorResult.put("rawResponse", "");
            return errorResult;
        }
    }
    
    private String buildPrompt(Map<String, Object> extractionData, List<Map<String, Object>> interactiveElements, String userContext) {
        try {
            // Convert interactive elements to clean JSON
            String elementsJson = json.writeValueAsString(interactiveElements);
            //print out the elementsJson for debugging
            System.out.println("Interactive Elements JSON: " + elementsJson);
            
            @SuppressWarnings("unchecked")
            Map<String, Object> summary = (Map<String, Object>) extractionData.get("summary");
            
            String systemPrompt =  String.format("""
                You are a web automation expert tasked with generating Playwright test steps for interactive web elements.

                TASK:
                Analyze the provided elements and generate a logical sequence of human-like interactions that accomplish the user's goal.
                Identify the relevant elements that you need to interact with to fulfill the user's goal directly and without unnecessary steps.

                INPUT YOU WILL RECEIVE:
                - Elements: (contains CSS selectors, coordinates, aria-labels, placeholders, purpose identifiers, and metadata)
                - Context:  number of total elements, and form elements
                - User Goal 
                - Screenshot: Cropped area containing these elements for visual context
                 
                METADATA AVAILABLE:
                Each element includes:
                - nearbyLabels: Text appearing within 100px with position indicators (above/below/left/right) and distance
                - parentContext: Parent container information (meaningful classes/IDs)
                - sectionHeader: Nearest heading element that provides context for this element
                - tableContext: If in a table, includes columnHeader, rowIndex, and columnIndex
               
                OUTPUT FORMAT:
                Return ONLY a valid JSON array with no markdown, explanations, or additional text.
                 
                [
                  {
                    "type": "CLICK",
                    "selector": "<css-selector>",
                    "coords": "coordinates in a string format x,y for example 200,300"
                    "description": "<brief action description>"
                  },
                  {
                    "type": "TYPE",
                    "selector": "<css-selector>",
                    "text": "<value to enter>",
                    "isPassword": true,
                    "description": "<brief action description>"
                  }
                ]
                 
                RULES:

                1. Element Identification (priority order):
                    - Check metadata in this order: nearbyLabels, aria-label, tableContext.columnHeader, sectionHeader, parentContext, purpose.
                    - nearbyLabels: main source of meaning, especially labels positioned above or left.
                    - aria-label: authoritative when present.
                    - tableContext.columnHeader: defines purpose in tables.
                    - sectionHeader: gives contextual grouping.
                    - parentContext: use parent class or ID hints such as “timesheet” or “calendar”.
                    - purpose: fallback identifier such as “email-field” or “submit-button”.
                    - Always cross-reference at least two metadata sources.
                    - Use coordinates exactly as provided.

                2. Action Selection:
                    - TYPE: for input fields, textareas, or editable elements.
                    - CLICK: for buttons, links, checkboxes, radios, or submits.
                    - Set isPassword to true if aria-label or purpose implies a password field.

                3. Field Detection and Value Choice:
                    - Use aria-label to determine what value to enter.
                    - If nearbyLabels or aria-label contain “Date”, “Calendar”, or time terms and the goal does not mention dates, skip the element.
                    - Use tableContext.columnHeader to match fields with the user goal.
                    - Generate TYPE actions for elements with purpose ending in “-field” or with input-related aria-labels.
                    - Choose input values consistent with the user goal.

                4. Interaction Flow:
                    - Follow natural user flow: fill fields first, then submit.
                    - Complete all required fields before submission.
                    - Generate no more than 15 concise, action-oriented steps.
                    - Use aria-label to interpret button or field meaning.

                5. Context Awareness:
                    - When aria-label, nearbyLabels, and tableContext.columnHeader are all present, treat them as the most reliable sources.
                    - Respect both visual and logical page order.
                    - Adapt interactions to match the user’s stated goal.
                    - Cross-validate all metadata before deciding.

                6. Disambiguation:
                    - When multiple elements are similar, choose the one whose nearbyLabels text most closely matches the goal terms.
                    - In tables, prefer elements whose columnHeader matches the goal.
                    - If multiple elements remain valid, select the one with shorter label distance or clearer metadata.

                Remember: Output must be a valid JSON array only. No additional formatting or explanation.
                """,
                elementsJson,
                extractionData.get("elementCount"),
                summary.get("hasForm"),
                userContext
            );

            String prompt = String.format("""
                - Elements: (contains CSS selectors, coordinates, aria-labels, placeholders, purpose identifiers, and metadata)
                - Context:  number of total elements, and form elements
                - User Goal 
                - Screenshot:
                Provide the Playwright test steps as a JSON array.
                """,
                systemPrompt,
                elementsJson,
                userContext
            );
        } catch (Exception e) {
            return "Generate Playwright test steps as JSON array for these elements: " + 
                   interactiveElements.toString();
        }
    }
    
    private String extractJsonArray(String response) {
        response = response.replaceAll("(?s)```json|```", "");

        int start = response.indexOf('[');
        int end = response.lastIndexOf(']');
        
        if (start >= 0 && end > start) {
            return response.substring(start, end + 1);
        }
        
        return response;
    }
}