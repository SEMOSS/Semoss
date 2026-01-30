package prerna.reactor.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class GenerateEngineMetadataReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateEngineMetadataReactor.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public GenerateEngineMetadataReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MODEL.getKey(),
				ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.OPTIONS.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = insight.getUser();
		String engineId = keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String modelEngineId = keyValue.get(ReactorKeysEnum.MODEL.getKey());

		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}

		if (modelEngineId == null || modelEngineId.isEmpty()) {
			throw new IllegalArgumentException("Model engineId is required");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);

		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("User does not have permission to edit engine");
		}

		try {
			IEngine engine = Utility.getEngine(engineId);

			List<String> metaKeys = getListString(ReactorKeysEnum.META_KEYS.getKey());
			if (metaKeys == null || metaKeys.isEmpty()) {
				// Default to common metadata fields if none specified
				metaKeys = Arrays.asList("description", "tags");
			}

			// Current metadata
			Map<String, Object> currentMetadata = SecurityEngineUtils.getAggregateEngineMetadata(engineId, metaKeys,
					false);

			// target fields
			Set<String> targetFields = new LinkedHashSet<>();
			for (String key : metaKeys) {
				targetFields.add(key);
			}

			Map<String, Object> options = getMap(ReactorKeysEnum.OPTIONS.getKey());

			if (options == null) {
				options = new HashMap<>();
			}
			boolean enhanceExistingDescription = Boolean.TRUE.equals(options.get("useExistingDescription"));

			if (targetFields.isEmpty() && !enhanceExistingDescription) {
				return new NounMetadata(
						"Nothing to generate or enhance; all requested metadata fields are already populated",
						PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			}

			Map<String, Object> llmPayload = buildLLMInput(engine, options, currentMetadata, engineId,
					enhanceExistingDescription);

			// Build prompt with full context
			String prompt = buildPrompt(targetFields, llmPayload, engine.getCatalogType(), options,
					enhanceExistingDescription);

			// Call LLM
			IModelEngine modelEngine = Utility.getModel(modelEngineId);
			Map<String, Object> response = modelEngine
					.ask(prompt, null, insight, Map.of("temperature", 0.3, "max_completion_tokens", 4000)).toMap();

			Map<String, Object> generated = parseResponse(response.get("response"));

			Map<String, Object> returnPayload = new HashMap<>();
			returnPayload.put("generated_metadata", generated);
			returnPayload.put("generated_fields", new ArrayList<>(targetFields));
			returnPayload.put("options_used", options);
			returnPayload.put("engine_type", engine.getCatalogType().toString());
			returnPayload.put("engine_name", engine.getEngineName());
			returnPayload.put("data_sent_summary", buildDataSentSummary(llmPayload));

			EngineSyncUtility.clearEngineCache(engineId);

			return new NounMetadata(returnPayload, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);

		} catch (Exception e) {
			classLogger.error("Engine metadata generation failed", e);
			return new NounMetadata("Failed to generate engine metadata: " + e.getMessage(), PixelDataType.CONST_STRING,
					PixelOperationType.ERROR);
		}
	}

	/**
	 * Build the LLM input payload with context-aware data for each catalog type
	 */
	private Map<String, Object> buildLLMInput(IEngine engine, Map<String, Object> options,
			Map<String, Object> currentMetadata, String engineId, boolean enhanceExistingDescription) throws Exception {

		Map<String, Object> input = new LinkedHashMap<>();
		IEngine.CATALOG_TYPE engineType = engine.getCatalogType();
		input.put("engineName", engine.getEngineName());
		input.put("engineType", engineType.toString());

		String tone = String.valueOf(options.getOrDefault("tone", "professional"));
		input.put("tone", tone);

		// use current description
		if (enhanceExistingDescription) {
			Object existingDescription = currentMetadata.get("description");
			if (!isEmpty(existingDescription)) {
				input.put("existingDescription", existingDescription);
			}
		}

		// Additional context from user
		Object additionalContext = options.get("additionalContext");
		if (additionalContext != null && !isEmpty(additionalContext)) {
			input.put("additionalContext", additionalContext);
		}

		// DATABASE context
		if (engineType == IEngine.CATALOG_TYPE.DATABASE) {
			IDatabaseEngine database = Utility.getDatabase(engineId);
			if (Boolean.TRUE.equals(options.get("includeSchema"))) {
				int tableLimit = getIntOption(options, "tableSchemaLimit", 5);
				int columnLimit = getIntOption(options, "columnSchemaLimit", 10);
				AbstractOWLEngine owlEngine = database.getOWLEngineFactory().getReadOWL();
				Map<String, List<String>> schema = new LinkedHashMap<>();

				List<String> concepts = owlEngine.getConcepts();
				int tableCount = 0;

				for (String conceptUri : concepts) {
					if (tableCount >= tableLimit) {
						break;
					}

					String tableName = Utility.getInstanceName(conceptUri);
					List<String> columns = new ArrayList<>();

					List<String> propertyUris = owlEngine.getPropertyUris4PhysicalUri(conceptUri);

					int columnCount = 0;
					for (String propertyUri : propertyUris) {
						if (columnCount >= columnLimit) {
							break;
						}

						columns.add(Utility.getClassName(propertyUri));
						columnCount++;
					}

					schema.put(tableName, columns);
					tableCount++;
				}

				input.put("schema", schema);
			}
		}

		// VECTOR context
		if (engine.getCatalogType() == IEngine.CATALOG_TYPE.VECTOR) {
			IVectorDatabaseEngine vector = Utility.getVectorDatabase(engineId);

			if (Boolean.TRUE.equals(options.get("includeVectorFileNames"))) {
				int limit = getIntOption(options, "vectorFileLimit", 5);
				try {
					List<Map<String, Object>> files = vector.listDocuments(new HashMap<>());
					List<String> names = new ArrayList<>();

					for (Map<String, Object> f : files) {
						if (names.size() >= limit) {
							break;
						}
						Object name = f.get("fileName");
						if (name != null) {
							names.add(name.toString());
						}
					}

					if (!names.isEmpty()) {
						input.put("vectorFiles", names);
					}
				} catch (Exception e) {
					classLogger.warn("Could not fetch vector file names", e);
				}
			}

			if (Boolean.TRUE.equals(options.get("includeVectorChunks"))) {
				int limit = getIntOption(options, "vectorChunkLimit", 10);
				try {
					List<Map<String, Object>> chunks = vector.listAllRecords(new HashMap<>());
					List<String> samples = new ArrayList<>();

					for (Map<String, Object> c : chunks) {
						if (samples.size() >= limit) {
							break;
						}
						Object content = c.get("Content");
						if (content != null) {
							String contentStr = content.toString();
							// Truncate long chunks
							if (contentStr.length() > 300) {
								contentStr = contentStr.substring(0, 300) + "...";
							}
							samples.add(contentStr);
						}
					}

					if (!samples.isEmpty()) {
						input.put("vectorChunkSamples", samples);
					}
				} catch (Exception e) {
					classLogger.warn("Could not fetch vector chunks", e);
				}
			}

		}

		// STORAGE context
		if (engine.getCatalogType() == IEngine.CATALOG_TYPE.STORAGE) {
			IStorageEngine storage = Utility.getStorage(engineId);

			if (Boolean.TRUE.equals(options.get("includeStorageFileNames"))) {
				int limit = getIntOption(options, "storageFileLimit", 10);
				try {
					List<String> files = storage.list("");
					List<String> names = new ArrayList<>();

					for (String f : files) {
						if (names.size() >= limit) {
							break;
						}
						if (f != null && !f.isEmpty()) {
							names.add(f);
						}
					}

					if (!names.isEmpty()) {
						input.put("storageFiles", names);
					}
				} catch (Exception e) {
					classLogger.warn("Could not fetch storage file names", e);
				}
			}
		}

		// MODEL context
		if (engine.getCatalogType() == IEngine.CATALOG_TYPE.MODEL) {
			IModelEngine modelEngine = Utility.getModel(engineId);

			if (Boolean.TRUE.equals(options.get("includeModelSmssInfo"))) {
				try {
					Properties modelInfo = modelEngine.getOrigSmssProp();

					Map<String, Object> filteredInfo = new LinkedHashMap<>();

					String[] keys = { "MODEL", "MODEL_TYPE" };

					for (String k : keys) {
						String val = modelInfo.getProperty(k);
						if (val != null) {
							filteredInfo.put(k, val);
						}
					}

					input.put("modelSmssInfo", filteredInfo);

				} catch (Exception e) {
					classLogger.warn("Could not fetch model smss info", e);
				}
			}
		}

		// FUNCTION context
		if (engine.getCatalogType() == IEngine.CATALOG_TYPE.FUNCTION) {
			if (Boolean.TRUE.equals(options.get("includeFunctionSmssInfo"))) {
				try {
					Properties funcSmssInfo = engine.getOrigSmssProp();
					Map<String, Object> filteredInfo = new LinkedHashMap<>();

					String[] keys = { "FUNCTION_DESCRIPTION", "FUNCTION_TYPE" };
					for (String k : keys) {
						String val = funcSmssInfo.getProperty(k);
						if (val != null) {
							filteredInfo.put(k, val);
						}
					}

					input.put("funcSmssInfo", filteredInfo);
				} catch (Exception e) {
					classLogger.warn("Could not fetch function smss info", e);
				}
			}
		}

		return input;
	}

	/**
	 * Build context-aware, specific prompts for each catalog type
	 */
	private String buildPrompt(Set<String> targetFields, Map<String, Object> llmPayload,
			IEngine.CATALOG_TYPE catalogType, Map<String, Object> options, boolean enhance) {

		StringBuilder prompt = new StringBuilder();

		// System instruction
		prompt.append("You are an expert data catalog specialist with deep knowledge of ");
		prompt.append(catalogType.toString().toLowerCase()).append(" systems. ");
		prompt.append(
				"Your task is to generate accurate, specific, and professional metadata that reflects the actual content and purpose.\n");

		// Context about the data source
		prompt.append("## Information\n");
		prompt.append("- **Name**: ").append(llmPayload.get("engineName")).append("\n");
		prompt.append("- **Type**: ").append(catalogType.toString()).append("\n");

		String tone = (String) llmPayload.getOrDefault("tone", "professional");
		prompt.append("- **Tone**: ").append(tone).append("\n");

		// Add catalog-type-specific context
		switch (catalogType) {
		case DATABASE:
			buildDatabasePromptContext(prompt, llmPayload);
			break;
		case VECTOR:
			buildVectorPromptContext(prompt, llmPayload);
			break;
		case STORAGE:
			buildStoragePromptContext(prompt, llmPayload);
			break;
		case MODEL:
			buildModelPromptContext(prompt, llmPayload);
			break;
		case FUNCTION:
			buildFunctionPromptContext(prompt, llmPayload);
			break;
		default:
			buildGenericPromptContext(prompt, llmPayload);
		}

		// User-provided context (always include if available)
		if (llmPayload.containsKey("additionalContext")) {
			prompt.append("## Additional Context from User\n");
			prompt.append(llmPayload.get("additionalContext")).append("\n");
		}

		// Existing description for enhancement
		if (enhance && llmPayload.containsKey("existingDescription")) {
			prompt.append("## Current Description (to enhance)\n");
			prompt.append(llmPayload.get("existingDescription")).append("\n");
		}

		// Instructions for generation
		prompt.append("## Generation Requirements\n");
		if (enhance) {
			prompt.append("Enhance the existing description by:\n");
			prompt.append("1. Making it more specific and detailed based on the provided context\n");
			prompt.append("2. Adding concrete details from the schema/files/data shown above\n");
			prompt.append("3. Avoiding generic phrases like 'this database contains' or 'this is a collection of'\n");
			prompt.append("4. Using active, specific language that describes the actual purpose and content\n");
		} else {
			prompt.append("Generate metadata for these fields: ").append(String.join(", ", targetFields)).append("\n");

			if (targetFields.contains("description")) {
				prompt.append("### Description Guidelines:\n");
				prompt.append("- Be SPECIFIC: Use actual table names, file types, or content topics from the data\n");
				prompt.append("- Be CONCRETE: Mention real entities, not generic terms\n");
				prompt.append("- Be PURPOSEFUL: Explain what this data source enables users to do\n");
				prompt.append("- AVOID generic phrases like:\n");
				prompt.append("  'This is a database that stores information'\n");
				prompt.append("  'A collection of data'\n");
				prompt.append("  'Contains various files'\n");
				prompt.append("- PREFER specific statements like:\n");
				prompt.append(
						"  'Customer transaction database tracking orders, payments, and shipments across 15 retail locations'\n");
				prompt.append(
						"  'Technical documentation vector store containing 500+ engineering specs and API guides'\n");
				prompt.append(
						"  'Financial reports storage with quarterly earnings, balance sheets, and compliance documents'\n");
			}

			if (targetFields.contains("tags")) {
				prompt.append("### Tags Guidelines:\n");
				prompt.append("- Extract 3-7 relevant tags\n");
				prompt.append("- Use specific domain terms from the actual content\n");
				prompt.append("- Include: data subject matter, use cases, departments, or industries\n");
				prompt.append("- Avoid overly generic tags like 'data', 'information', 'files'\n");
				prompt.append(
						"- Examples: 'customer-analytics', 'financial-reporting', 'ml-training-data', 'api-documentation'\n");
			}
		}

		// Output format
		prompt.append("## Output Format\n");
		prompt.append("Return ONLY valid JSON with this exact structure:\n");
		prompt.append("```json\n");
		prompt.append("{\n");

		if (targetFields.contains("description")) {
			prompt.append("  \"description\": \"Your specific, detailed description here\",\n");
		}
		if (targetFields.contains("tags")) {
			prompt.append("  \"tags\": [\"tag1\", \"tag2\", \"tag3\"]\n");
		}

		// Handle custom fields
		for (String field : targetFields) {
			if (!field.equals("description") && !field.equals("tags")) {
				prompt.append("  \"").append(field).append("\": \"value\",\n");
			}
		}

		prompt.append("}\n```\n\n");
		prompt.append(
				"Remember: Be specific, be accurate, avoid generic language. Use the actual data provided above.");

		return prompt.toString();
	}

	/**
	 * Build DATABASE-specific prompt context
	 */
	private void buildDatabasePromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("## Database Schema Context\n");

		if (llmPayload.containsKey("schema")) {
			@SuppressWarnings("unchecked")
			Map<String, List<String>> schema = (Map<String, List<String>>) llmPayload.get("schema");

			prompt.append("This database contains ").append(schema.size()).append(" tables:\n");

			for (Map.Entry<String, List<String>> entry : schema.entrySet()) {
				String tableName = entry.getKey();
				List<String> columns = entry.getValue();

				prompt.append("### Table: `").append(tableName).append("`\n");
				prompt.append("Columns (").append(columns.size()).append("): ");
				prompt.append(String.join(", ", columns.stream().limit(15).collect(Collectors.toList())));
				if (columns.size() > 15) {
					prompt.append("... (").append(columns.size() - 15).append(" more)");
				}
				prompt.append("\n");
			}

			prompt.append(
					"**Key Insight**: Based on these table and column names, identify the domain (e.g., e-commerce, healthcare, finance) ");
			prompt.append("and describe specific entities and relationships this database manages.\n");
		} else {
			prompt.append("(Schema not provided - describe based on engine name and any other context)\n");
		}
	}

	/**
	 * Build VECTOR-specific prompt context
	 */
	private void buildVectorPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("## Vector Database Context\n");

		if (llmPayload.containsKey("vectorFiles")) {
			@SuppressWarnings("unchecked")
			List<String> files = (List<String>) llmPayload.get("vectorFiles");

			prompt.append("### Indexed Documents (sample of ").append(files.size()).append("):\n");
			for (String file : files) {
				prompt.append("- ").append(file).append("\n");
			}
			prompt.append("\n**Analysis Required**: What domain or subject matter do these documents cover? ");
			prompt.append("What knowledge areas or use cases does this vector database support?\n");
		}

		if (llmPayload.containsKey("vectorChunkSamples")) {
			@SuppressWarnings("unchecked")
			List<String> chunks = (List<String>) llmPayload.get("vectorChunkSamples");

			prompt.append("### Content Samples (").append(chunks.size()).append(" chunks):\n");
			for (int i = 0; i < Math.min(5, chunks.size()); i++) {
				prompt.append("```\n").append(chunks.get(i)).append("\n```\n\n");
			}

			prompt.append("**Key Insight**: Analyze these content samples to identify:\n");
			prompt.append("- Subject matter and topics\n");
			prompt.append("- Technical level (beginner, advanced, expert)\n");
			prompt.append("- Use cases (Q&A, semantic search, recommendations)\n");
			prompt.append("- Target audience\n");
		}
	}

	/**
	 * Build STORAGE-specific prompt context
	 */
	private void buildStoragePromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("## Storage Context\n");

		if (llmPayload.containsKey("storageFiles")) {
			@SuppressWarnings("unchecked")
			List<String> files = (List<String>) llmPayload.get("storageFiles");

			prompt.append("### Sample Files (").append(files.size()).append("):\n");
			for (String file : files) {
				prompt.append("- ").append(file).append("\n");
			}
			prompt.append("\n**Analysis Required**: Based on file names and types:\n");
			prompt.append("- What type of content is stored? (documents, images, datasets, reports, etc.)\n");
			prompt.append("- What business function does this support? (reporting, archival, collaboration, etc.)\n");
			prompt.append("- Who are the likely users or consumers?\n");
		}
	}

	/**
	 * Build MODEL-specific prompt context
	 */
	private void buildModelPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("## Model Context\n");

		if (llmPayload.containsKey("modelSmssInfo")) {
			@SuppressWarnings("unchecked")
			Map<String, Object> modelSmssInfo = (Map<String, Object>) llmPayload.get("modelSmssInfo");

			if (modelSmssInfo.containsKey("MODEL_TYPE")) {
				prompt.append("Model Type: ").append(modelSmssInfo.get("MODEL_TYPE")).append("\n");
			}
			if (modelSmssInfo.containsKey("MODEL")) {
				prompt.append("Model: ").append(modelSmssInfo.get("MODEL")).append("\n");
			}
			prompt.append("\n");
		}

		prompt.append("**Description Guidelines for AI Models**:\n");
		prompt.append("- Specify the model type (LLM, embedding, classification, etc.)\n");
		prompt.append("- Mention capabilities (text generation, Q&A, sentiment analysis, etc.)\n");
		prompt.append("- Indicate use cases or applications\n");
	}

	/**
	 * Build FUNCTION-specific prompt context
	 */
	private void buildFunctionPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("## Function Context\n");

		if (llmPayload.containsKey("funcSmssInfo")) {
			@SuppressWarnings("unchecked")
			Map<String, Object> funcSmssInfo = (Map<String, Object>) llmPayload.get("funcSmssInfo");

			if (funcSmssInfo.containsKey("FUNCTION_DESCRIPTION")) {
				prompt.append("Function Description: ").append(funcSmssInfo.get("FUNCTION_DESCRIPTION")).append("\n");
			}
			if (funcSmssInfo.containsKey("FUNCTION_TYPE")) {
				prompt.append("Function Type: ").append(funcSmssInfo.get("FUNCTION_TYPE")).append("\n");
			}
		}

		prompt.append("**Description Guidelines for Functions**:\n");
		prompt.append("- Describe what the function does (inputs, outputs)\n");
		prompt.append("- Mention use cases or workflows it supports\n");
	}

	/**
	 * Build generic prompt context for other catalog types
	 */
	private void buildGenericPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("## Context\n");
		prompt.append(
				"Based on the engine name and type, provide a specific, accurate description of its purpose and contents.\n");
	}

	/**
	 * Parse LLM response and extract JSON metadata
	 */
	private Map<String, Object> parseResponse(Object response) {
		if (response == null) {
			return new HashMap<>();
		}

		try {
			String responseStr = response.toString();

			// Try to extract JSON from response (handle markdown code blocks)
			int jsonStart = responseStr.indexOf("{");
			int jsonEnd = responseStr.lastIndexOf("}") + 1;

			if (jsonStart >= 0 && jsonEnd > jsonStart) {
				String jsonStr = responseStr.substring(jsonStart, jsonEnd);

				// Parse JSON
				@SuppressWarnings("unchecked")
				Map<String, Object> parsed = MAPPER.readValue(jsonStr, Map.class);

				// Validate and clean the response
				Map<String, Object> cleaned = new HashMap<>();

				if (parsed.containsKey("description")) {
					String desc = String.valueOf(parsed.get("description"));
					if (!isEmpty(desc)) {
						cleaned.put("description", desc.trim());
					}
				}

				if (parsed.containsKey("tags")) {
					Object tagsObj = parsed.get("tags");
					if (tagsObj instanceof List) {
						@SuppressWarnings("unchecked")
						List<String> tags = ((List<?>) tagsObj).stream()
								.filter(t -> t != null && !isEmpty(t.toString())).map(Object::toString)
								.map(String::trim).collect(Collectors.toList());

						if (!tags.isEmpty()) {
							cleaned.put("tags", tags);
						}
					}
				}

				// Include any other custom fields
				for (Map.Entry<String, Object> entry : parsed.entrySet()) {
					String key = entry.getKey();
					if (!key.equals("description") && !key.equals("tags") && entry.getValue() != null) {
						cleaned.put(key, entry.getValue());
					}
				}

				return cleaned;
			}

			classLogger.warn("No JSON found in LLM response");
			return new HashMap<>();

		} catch (Exception e) {
			classLogger.error("Failed to parse LLM response as JSON", e);
			return new HashMap<>();
		}
	}

	/**
	 * Build summary of what data was sent to LLM for transparency
	 */
	private Map<String, Object> buildDataSentSummary(Map<String, Object> llmPayload) {
		Map<String, Object> summary = new LinkedHashMap<>();

		summary.put("engineName", llmPayload.containsKey("engineName"));
		summary.put("engineType", llmPayload.containsKey("engineType"));
		summary.put("schema", llmPayload.containsKey("schema"));
		summary.put("vectorFiles", llmPayload.containsKey("vectorFiles"));
		summary.put("vectorChunkSamples", llmPayload.containsKey("vectorChunkSamples"));
		summary.put("storageFiles", llmPayload.containsKey("storageFiles"));
		summary.put("modelSmssInfo", llmPayload.containsKey("modelSmssInfo"));
		summary.put("funcSmssInfo", llmPayload.containsKey("funcSmssInfo"));
		summary.put("additionalContext", llmPayload.containsKey("additionalContext"));
		summary.put("existingDescription", llmPayload.containsKey("existingDescription"));
		return summary;
	}

	/**
	 * Helper method to get integer option with default
	 */
	private int getIntOption(Map<String, Object> options, String key, int defaultVal) {
		Object value = options.get(key);
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		if (value instanceof String) {
			try {
				return Integer.parseInt((String) value);
			} catch (NumberFormatException e) {
				// Ignore and return default
			}
		}
		return defaultVal;
	}

	/**
	 * Helper method to check if value is empty
	 */
	private boolean isEmpty(Object value) {
		if (value == null) {
			return true;
		}
		if (value instanceof String) {
			return ((String) value).trim().isEmpty();
		}
		if (value instanceof Collection) {
			return ((Collection<?>) value).isEmpty();
		}
		if (value instanceof Map) {
			return ((Map<?, ?>) value).isEmpty();
		}
		return false;
	}

	@Override
	public String getReactorDescription() {
		return "Generates or enhances engine metadata using an LLM, based on engine context and configurable options. "
				+ "User can control what context is sent to the LLM.";

	}

	@Override
	protected String getDescriptionForKey(String key) {

		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Unique engine identifier for which metadata will be generated.";
		}

		if (key.equals(ReactorKeysEnum.MODEL.getKey())) {
			return "Model engine identifier used to generate metadata.";
		}

		if (key.equals(ReactorKeysEnum.META_KEYS.getKey())) {
			return "List of metadata field names to generate" + "If not provided, defaults to ['description', 'tags'].";
		}

		if (key.equals(ReactorKeysEnum.OPTIONS.getKey())) {
			return """
					Optional configuration map that controls how engine metadata is generated.

					Supported options:

					1) Database
					- includeSchema (boolean):
					  Include database schema details such as tables and columns.
					- tableSchemaLimit (number):
					  Max tables to include (default: 5)
					- columnSchemaLimit (number):
					  Max columns per table (default: 10)

					2) Vectors
					- includeVectorFileNames (boolean):
					  Include vector document file names.
					- vectorFileLimit (number):
					  Maximum number of vector file names to include (default: 5).
					- includeVectorChunks (boolean):
					  Include sample text chunks from vector documents.
					- vectorChunkLimit (number):
					  Maximum number of vector text chunks to include (default: 3).

					3) Storages
					- includeStorageFileNames (boolean):
					  Include storage file names.
					- storageFileLimit (number):
					  Maximum number of storage file names to include (default: 5).

					4) Models
					- includeModelSmssInfo (boolean):
					  Include model smss file content.

					5) Functions
					- includeFunctionSmssInfo (boolean):
					  Include function smss file content.

					6) User-specific options
					- useExistingDescription (boolean):
					  Refine and enhance an existing description instead of generating a new one.
					- additionalContext (string):
					  Extra user-provided context to guide metadata generation.
					- tone (string):
					  Writing tone for the generated description.
					  Supported values: neutral | business | scientific (default: neutral).
					""";
		}

		return super.getDescriptionForKey(key);
	}

}
