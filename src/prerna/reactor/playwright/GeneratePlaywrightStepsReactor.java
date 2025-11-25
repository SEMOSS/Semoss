package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GeneratePlaywrightStepsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GeneratePlaywrightStepsReactor.class);

	private ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public GeneratePlaywrightStepsReactor() {
		this.keysToGet = new String[] { "engine", "sessionId", "roomId", ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String roomId = this.keyValue.get(this.keysToGet[2]);
		Map<String, Object> paramValues = getMap(this.keysToGet[3]);

		Map<String, Object> result = generateSteps(engineId, sessionId, roomId, paramValues);
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> generateSteps(String engineId, String sessionId, String roomId,
			Map<String, Object> params) {
		try {
			// Get the HTML extraction data
			Map<String, Object> extractionData = (Map<String, Object>) params.get("extractionData");

			if (extractionData == null) {
				throw new IllegalArgumentException("extractionData is required");
			}

			String userContext = (String) params.getOrDefault("userContext", " ");
			if (userContext == null || userContext.trim().isEmpty()) {
				userContext = " ";
			}

			// cropParams
			Map<String, Object> cropParams = (Map<String, Object>) params.get("cropParams");
			ScreenshotResponse croppedImage = ScreenshotReactor.croppedScreenshot(
					this.insight.getUser().getPlaywrightSession(sessionId), "tab-1",
					((Number) cropParams.get("startX")).intValue(), ((Number) cropParams.get("startY")).intValue(),
					((Number) cropParams.get("endX")).intValue(), ((Number) cropParams.get("endY")).intValue());

			// Get interactive elements only
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> allElements = (List<Map<String, Object>>) extractionData.get("elements");
			List<Map<String, Object>> interactiveElements = allElements.stream()
					.filter(e -> Boolean.TRUE.equals(e.get("interactive"))).toList();

			String prompt = buildPrompt(extractionData, interactiveElements, userContext);

			IModelEngine modelEngine = Utility.getModel(engineId);

			String insightFolder = this.insight.getInsightFolder();
			String imageName = "playwright_screenshot_generation_" + System.currentTimeMillis() + ".png";

			if (roomId == null || roomId.isEmpty()) {
				roomId = UUID.randomUUID().toString();
			}
			String modelOutput = PlaywrightUtility.callModel(insightFolder, imageName, croppedImage, modelEngine,
					prompt, this.insight, roomId);

			// Try to extract JSON array from the response
			String cleanedResponse = extractJsonArray(modelOutput);

			Map<String, Object> result = new HashMap<>();
			result.put("rawResponse", modelOutput);
			result.put("stepsJson", cleanedResponse);
			result.put("success", true);

			return result;

		} catch (Exception e) {
			classLogger.error("Error generating playwright steps: " + e.getMessage(), e);
			Map<String, Object> errorResult = new HashMap<>();
			errorResult.put("success", false);
			errorResult.put("error", e.getMessage());
			errorResult.put("rawResponse", "");
			return errorResult;
		}
	}

	@SuppressWarnings("unchecked")
	private String buildPrompt(Map<String, Object> extractionData, List<Map<String, Object>> interactiveElements,
			String userContext) {
		try {
			// Convert interactive elements to clean JSON
			String elementsJson = json.writeValueAsString(interactiveElements);

			Map<String, Object> summary = (Map<String, Object>) extractionData.get("summary");

			return String.format(
					"""
							You are a web automation expert tasked with generating Playwright test steps for interactive web elements.
							You are only allowed to generate "CLICK" and "TYPE" actions for the following elements.

							INPUT:
							- Elements: %s (contains CSS selectors, coordinates, aria-labels, placeholders, sectionHeader, tableContext ,and purpose identifiers)
							- Context: %d total elements, %s form elements
							- User Goal: %s
							- Screenshot: Cropped area containing these elements for visual context

							TASK:
							Analyze the provided elements and generate a logical sequence of human-like interactions that accomplish the user's goal.
							Identify the relevant elements that you need to interact with to fulfill the user's goal directly and without unnecessary steps.

							OUTPUT FORMAT:
							Return ONLY a valid JSON array with no markdown, explanations, or additional text.

							[
							  {
							    "type": "CLICK",
							    "selector": "<css-selector>",
							    "coordinates": "coordinates of element sent as x,y"
							    "description": "<brief action description>"
							  },
							  {
							    "type": "TYPE",
							    "selector": "<css-selector>",
							    "text": "<value to enter>",
							    "isPassword": true,
							    "coordinates": "coordinates of element sent as x,y"
							    "description": "<brief action description>"
							  }
							]

							RULES:
							1. Element Identification (Priority Order):
							   - **PRIMARY: aria-label** - If present, use aria-label as the authoritative source for understanding element purpose and meaning
							   - SECONDARY: "purpose" field - Use as identifier (e.g., "email-field", "password-field", "submit-button")
							   - TERTIARY: placeholder text, sectionHeader, tableContext - Use for additional context when aria-label is absent
							   - ALWAYS prioritize aria-label over other attributes when it exists
							   - Use coordinates and selectors EXACTLY as provided in the element data

							2. Action Types:
							    - TYPE: MUST be for INPUT elements or TEXT FIELD elements only.
							    - CLICK: Use for buttons, links, checkboxes, radio buttons, and submit actions
							    - Set "isPassword": true when aria-label or purpose indicates password field

							3. Field Detection:
							    - Check aria-label FIRST to determine field type and required value
							    - If aria-label exists, let it guide what value should be entered
							    - Generate TYPE actions for elements with purpose ending in "-field" or aria-label indicating input
							    - Extract appropriate values based on aria-label meaning and user goal

							4. Interaction Logic:
							    - Follow natural user flow: fill form fields first, then submit
							    - If a form is present, complete all required fields before submission
							    - Generate up to 15 steps maximum
							    - Keep descriptions concise and action-oriented
							    - Use aria-label to understand button actions and field requirements

							5. Context Awareness:
							- **CRITICAL: When aria-label is present, it provides the most reliable understanding of element function**
							- Consider the visual layout from the screenshot
							- Respect the logical sequence of elements on the page
							- Adapt interactions to match the user's stated goal
							- Cross-reference aria-label with other attributes for complete understanding

							Remember: Output must be a valid JSON array only. No additional formatting or explanation.
							""",
					elementsJson, extractionData.get("elementCount"), summary.get("hasForm"), userContext);
		} catch (Exception e) {
			classLogger.error("Error building prompt for LLM: " + e.getMessage(), e);
			return "Error: Failed to build prompt for LLM. Details: " + e.getMessage();
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

	@Override
	public String getReactorDescription() {
		return "Generates Playwright steps (CLICK and TYPE actions) using an LLM based on extracted webpage elements and a user goal.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("engine")) {
			return "The id of the Model Engine";
		} else if (key.equals("roomId")) {
			return "The id of the room to call the model prompt with the context of the room for generating steps";
		}

		return super.getDescriptionForKey(key);
	}

}
