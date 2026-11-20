/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
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

		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		if (modelEngineId == null || (modelEngineId = modelEngineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Model engineId is required");
		}

		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("User does not have permission to edit engine");
		}
		if (!SecurityEngineUtils.userCanViewEngine(user, modelEngineId)) {
			throw new IllegalArgumentException("User does not have permission to model engine");
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

			Map<String, Object> options = getMap(ReactorKeysEnum.OPTIONS.getKey());

			if (options == null) {
				options = new HashMap<>();
			}
			boolean enhanceExistingDescription = Boolean.TRUE.equals(options.get("useExistingDescription"));

			// Only generate fields that are currently empty, unless user explicitly asked
			// to enhance an existing description.
			Set<String> targetFields = new LinkedHashSet<>();
			for (String key : metaKeys) {
				if ("description".equals(key) && enhanceExistingDescription) {
					targetFields.add(key);
					continue;
				}
				if (isEmpty(currentMetadata.get(key))) {
					targetFields.add(key);
				}
			}

			if (targetFields.isEmpty()) {
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
			Map<String, Object> llmParams = new HashMap<>();
			llmParams.put("temperature", 0.3);
			llmParams.put("max_completion_tokens", 4000);
			llmParams.put("response_format", buildResponseSchema(targetFields));

			Map<String, Object> response = modelEngine.ask(prompt, null, insight, llmParams).toMap();

			Map<String, Object> generated = parseResponse(response.get("response"), targetFields);

			Map<String, Object> returnPayload = new HashMap<>();
			returnPayload.put("generated_metadata", generated);
			returnPayload.put("generated_fields", new ArrayList<>(targetFields));
			returnPayload.put("options_used", options);
			returnPayload.put("engine_type", engine.getCatalogType().toString());
			returnPayload.put("engine_name", engine.getEngineName());
			returnPayload.put("data_sent_summary", buildDataSentSummary(llmPayload));
			if (!isEmpty(generated.get("explanation"))) {
				returnPayload.put("explanation", generated.get("explanation"));
			}

			return new NounMetadata(returnPayload, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);
		} catch (Exception e) {
			classLogger.error("Engine metadata generation failed", e);
			return new NounMetadata("Failed to generate engine metadata: " + e.getMessage(), PixelDataType.CONST_STRING,
					PixelOperationType.ERROR);
		}
	}

	/**
	 * Build the LLM input payload with context-aware data for each catalog type
	 * 
	 * @param engine
	 * @param options
	 * @param currentMetadata
	 * @param engineId
	 * @param enhanceExistingDescription
	 * @return
	 * @throws Exception
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
		else if (engine.getCatalogType() == IEngine.CATALOG_TYPE.VECTOR) {
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
		else if (engine.getCatalogType() == IEngine.CATALOG_TYPE.STORAGE) {
			IStorageEngine storage = Utility.getStorage(engineId);

			String storagePath = null;
			Object storagePathOption = options.get("storagePath");
			if (storagePathOption != null) {
				storagePath = storagePathOption.toString().trim();
			}
			if (storagePath == null || (storagePath = storagePath.trim()).isEmpty()) {
				storagePath = "/";
			}

			if (Boolean.TRUE.equals(options.get("includeStorageFileNames"))) {
				int limit = getIntOption(options, "storageFileNameLimit", 5);
				try {
					List<String> files = storage.list(storagePath);
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
			if (Boolean.TRUE.equals(options.get("includeStorageFileContent"))) {

				int fileLimit = getIntOption(options, "storageFileLimit", 3);
				int charLimit = getIntOption(options, "storageCharLimit", 500);

				String baseFileLocation = Utility.normalizePath(this.insight.getInsightFolder());
				File baseLocalDir = new File(baseFileLocation);
				if (!baseLocalDir.exists()) {
					baseLocalDir.mkdirs();
				}
				File localDir = new File(baseLocalDir, "engine_metadata_" + Utility.getRandomString(8));
				if (!localDir.exists()) {
					localDir.mkdirs();
				}

				try {
					List<String> candidatePaths = getStorageCandidateFilePaths(storage, storagePath, fileLimit);
					for (String candidatePath : candidatePaths) {
						try {
							storage.copyToLocal(candidatePath, localDir.getAbsolutePath());
						} catch (Exception e) {
							classLogger.warn("Could not copy storage file for metadata sampling: " + candidatePath, e);
						}
					}

					List<Map<String, String>> fileContents = new ArrayList<>();
					List<File> localFiles = new ArrayList<>();
					collectReadableFiles(localDir, localFiles, fileLimit);

					int count = 0;
					for (File f : localFiles) {
						if (count >= fileLimit) {
							break;
						}

						try {
							String content = null;
							String name = f.getName().toLowerCase();
							if (name.endsWith(".txt") || name.endsWith(".csv") || name.endsWith(".md")) {
								content = FileUtils.readFileToString(f, StandardCharsets.UTF_8);
							} else if (name.endsWith(".pdf")) {
								content = readPdf(f);
							} else if (name.endsWith(".docx")) {
								content = readDocX(f);
							} else if (name.endsWith(".doc")) {
								content = readDoc(f);
							} else {
								throw new IllegalArgumentException("Unsupported file type: " + f.getName());
							}

							if (content != null && content.length() > charLimit) {
								content = content.substring(0, charLimit) + "...";
							}

							Map<String, String> fileData = new HashMap<>();
							fileData.put("fileName", getRelativePath(localDir, f));
							fileData.put("content", content == null ? "" : content);

							fileContents.add(fileData);
							count++;

						} catch (Exception e) {
							classLogger.warn("Error reading file: " + f.getName(), e);
						}
					}

					if (!fileContents.isEmpty()) {
						input.put("storageFileSamples", fileContents);
					}

				} catch (Exception e) {
					classLogger.warn("Could not fetch storage file content", e);
				}
			}
		}

		// MODEL context
		else if (engine.getCatalogType() == IEngine.CATALOG_TYPE.MODEL) {
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
		else if (engine.getCatalogType() == IEngine.CATALOG_TYPE.FUNCTION) {
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
	 * 
	 * @param targetFields
	 * @param llmPayload
	 * @param catalogType
	 * @param options
	 * @param enhance
	 * @return
	 */
	private String buildPrompt(Set<String> targetFields, Map<String, Object> llmPayload,
			IEngine.CATALOG_TYPE catalogType, Map<String, Object> options, boolean enhance) {

		StringBuilder prompt = new StringBuilder();

		// Role Definition
		prompt.append(String.format(
				"""
						### ROLE
						You are a senior enterprise metadata strategist specialized in %s systems.
						Your objective is to produce concrete metadata that explains what this asset contains and the business outcomes it enables.

							""",
				catalogType.toString()));
		// Data Source Context
		String tone = (String) llmPayload.getOrDefault("tone", "professional");
		prompt.append(String.format("""
				### CONTEXT
				- **Type**: %s
				- **Tone**: %s
				""", catalogType.toString(), tone));

		prompt.append(
				"""
						### NON-NEGOTIABLE RULES
						1. BUSINESS-FIRST: Focus on business utility before technical implementation details.
						2. EVIDENCE-BASED: Every claim must be grounded in the provided context (table names, columns, files, excerpts, model/function metadata, or user context).
						3. NO GENERIC FILLER: Avoid empty phrases like "contains data", "collection of files", "knowledge base", "can be queried with SQL", or "stores information" unless tied to specific business use.
						4. ADMIT GAPS: If business intent is unclear, state the uncertainty explicitly and provide the best evidence-backed interpretation.
						5. OUTCOME LANGUAGE: Describe decisions, workflows, or operational actions this asset supports.
						""");

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
			prompt.append(String.format("""
					## Additional Context from User
					%s
					""", llmPayload.get("additionalContext")));
		}

		// Existing description for enhancement
		if (enhance && llmPayload.containsKey("existingDescription")) {
			prompt.append(String.format("""
					## Current Description (to enhance)
					%s
					""", llmPayload.get("existingDescription")));
		}

		// Generation Instructions
		prompt.append("### INSTRUCTIONS\n");
		if (enhance) {

			prompt.append(String.format(
					"""
								Enhance the existing description by following these requirements:
								1. SPECIFICITY: Incorporate concrete entities from context (tables, columns, document topics/types, identifiers).
								2. BUSINESS UTILITY: Explain who uses this asset and what workflows or decisions it enables.
								3. CLARITY: Use active voice and remove generic statements that only describe technology.
								4. ALIGNMENT: Ensure the tone remains %s throughout.
								5. EVIDENCE: Keep only claims that can be justified by provided context.

							""",
					tone));
		} else {
			prompt.append(String.format("Generate metadata for the following fields: %s\n\n",
					String.join(", ", targetFields)));

			if (targetFields.contains("description")) {
				prompt.append(
						"""
								#### Description Requirements:
									1. STRUCTURE: Write 3-5 sentences that cover:
									   - what content/domain is inside this asset,
									   - what business workflows/decisions it supports,
									   - who would use it and why it is valuable.
									2. EVIDENCE: Reference concrete entities from context (table/column names, file content themes, document types, excerpt themes, model/function identifiers).
									3. BUSINESS VALUE: Describe practical outcomes (reporting, compliance, forecasting, triage, customer operations, finance ops, etc.) not just storage/query mechanics.
									4. NO GENERIC TECHNOLOGY LANGUAGE: Avoid descriptions such as "relational database you can query with SQL", "contains data", "collection of files", or "knowledge base" unless followed by specific business purpose.
									5. PRECISION: If context is limited, say what is known and what is uncertain instead of inventing details.
									6. QUALITY BAR: The description must still make sense if words like "database", "vector", or "storage" are removed.

								""");
			}

			if (targetFields.contains("tags")) {
				prompt.append("""
						#### Tag Requirements:
						1. Extract 3-7 high-relevance tags.
						2. Use industry-standard domain terms.
						3. Avoid overly broad tags like 'data' or 'database'.

						""");
			}
		}

		// Output Format
		prompt.append(
				"""
						### OUTPUT FORMAT
						Return ONLY a valid JSON object. No conversational text or markdown explanation outside the JSON block.
						If context is insufficient, still return valid JSON using the same keys; put the limitation reason in an "explanation" field and do not return any values to the other fields (empty string or empty array).
						```json
						{
						""");

		List<String> outputLines = new ArrayList<>();
		if (targetFields.contains("description")) {
			outputLines.add("\"description\": \"Your specific, detailed description here\"");
		}
		if (targetFields.contains("tags")) {
			outputLines.add("\"tags\": [\"tag1\", \"tag2\", \"tag3\"]");
		}

		// Handle custom fields
		List<String> customFields = targetFields.stream()
				.filter(field -> !field.equals("description") && !field.equals("tags")).collect(Collectors.toList());
		for (String field : customFields) {
			outputLines.add("\"" + field + "\": \"value\"");
		}
		// add explanation
		outputLines.add("\"explanation\": \"Optional field in case of issues or context to return to the user\"");

		for (int i = 0; i < outputLines.size(); i++) {
			prompt.append("  ").append(outputLines.get(i));
			if (i < outputLines.size() - 1) {
				prompt.append(",");
			}
			prompt.append("\n");
		}

		prompt.append("""
				}
				```

				Remember: prioritize business utility, stay evidence-grounded, and avoid generic language.
				""");

		return prompt.toString();
	}

	/**
	 * Build DATABASE-specific prompt context
	 * 
	 * @param prompt
	 * @param llmPayload
	 */
	private void buildDatabasePromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("#### Relational Data Scope\n");
		prompt.append(
				"Infer likely business processes from the table and column semantics, not just schema structure.\n");

		if (llmPayload.containsKey("schema")) {

			Map<String, List<String>> schema = (Map<String, List<String>>) llmPayload.get("schema");

			prompt.append("Key tables and representative columns:\n");

			for (Map.Entry<String, List<String>> entry : schema.entrySet()) {
				String tableName = entry.getKey();
				List<String> columns = entry.getValue();

				prompt.append("- Table `").append(tableName).append("`: ");
				prompt.append(String.join(", ", columns.stream().limit(10).collect(Collectors.toList())));
				if (columns.size() > 10) {
					prompt.append(" (and ").append(columns.size() - 10).append(" more)");
				}
				prompt.append("\n");
			}

		} else {
			prompt.append("- Schema metadata unavailable.\n");
		}
	}

	/**
	 * Build VECTOR-specific prompt context
	 * 
	 * @param prompt
	 * @param llmPayload
	 */
	private void buildVectorPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("#### Knowledge Base Content\n");
		prompt.append(
				"Identify dominant business topics, recurring entities, and likely enterprise use cases from the excerpts.\n");

		if (llmPayload.containsKey("vectorFiles")) {

			List<String> files = (List<String>) llmPayload.get("vectorFiles");

			prompt.append("Referenced documents:\n");
			for (String file : files) {
				prompt.append("- ").append(file).append("\n");
			}
		}

		if (llmPayload.containsKey("vectorChunkSamples")) {

			List<String> chunks = (List<String>) llmPayload.get("vectorChunkSamples");
			int excerptCount = Math.min(3, chunks.size());

			prompt.append("\nContent excerpts (analyze ALL excerpts below):\n");
			for (int i = 0; i < excerptCount; i++) {
				prompt.append("Excerpt ").append(i + 1).append(": \"").append(chunks.get(i)).append("\"\n");
			}

			prompt.append("\n**CRITICAL INSTRUCTION**: Your description MUST synthesize information from ALL ")
					.append(excerptCount)
					.append(" excerpts above. Identify recurring themes and business purpose across all excerpts, not just the first.\n");
		}
	}

	/**
	 * Build STORAGE-specific prompt context
	 */
	private void buildStoragePromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("#### File Repository Content\n");
		prompt.append(
				"Infer what business operations these files support based on structure and content snippets. Treat filenames as weak signals only.\n");
		prompt.append(
				"Do not quote or enumerate literal filenames in the final description or tags unless explicitly required for business meaning.\n");

		if (llmPayload.containsKey("storageFiles")) {

			List<String> files = (List<String>) llmPayload.get("storageFiles");

			prompt.append("Filename context provided for ").append(files.size())
					.append(" files (use only as supporting hints, not as the main basis of the description).\n");
		}

		if (llmPayload.containsKey("storageFileSamples")) {

			List<Map<String, String>> samples = (List<Map<String, String>>) llmPayload.get("storageFileSamples");

			prompt.append("\nContent samples from files (analyze ALL files below):\n");
			for (int i = 0; i < samples.size(); i++) {
				Map<String, String> sample = samples.get(i);
				prompt.append("Sample ").append(i + 1).append(" Content Snippet: \"").append(sample.get("content"))
						.append("\"\n\n");
			}
			prompt.append("**CRITICAL INSTRUCTION**: Your description MUST synthesize information from ALL "
					+ samples.size()
					+ " file samples above. Identify recurring topics, document types, structure, and business purpose across all provided samples. Do not describe only the first sample and do not anchor the description to literal filenames.\n");
		}

	}

	/**
	 * Build MODEL-specific prompt context
	 * 
	 * @param prompt
	 * @param llmPayload
	 */
	private void buildModelPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("#### Model Capability Context\n");

		if (llmPayload.containsKey("modelSmssInfo")) {

			Map<String, Object> modelSmssInfo = (Map<String, Object>) llmPayload.get("modelSmssInfo");

			if (modelSmssInfo.containsKey("MODEL_TYPE")) {
				prompt.append("- Type: ").append(modelSmssInfo.get("MODEL_TYPE")).append("\n");
			}
			if (modelSmssInfo.containsKey("MODEL")) {
				prompt.append("- Provider/Architecture: ").append(modelSmssInfo.get("MODEL")).append("\n");
			}
		}

	}

	/**
	 * Build FUNCTION-specific prompt context
	 * 
	 * @param prompt
	 * @param llmPayload
	 */
	private void buildFunctionPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("#### Functional Logic Scope\n");

		if (llmPayload.containsKey("funcSmssInfo")) {
			Map<String, Object> funcSmssInfo = (Map<String, Object>) llmPayload.get("funcSmssInfo");

			if (funcSmssInfo.containsKey("FUNCTION_DESCRIPTION")) {
				prompt.append("- Description: ").append(funcSmssInfo.get("FUNCTION_DESCRIPTION")).append("\n");
			}
			if (funcSmssInfo.containsKey("FUNCTION_TYPE")) {
				prompt.append("- Implementation: ").append(funcSmssInfo.get("FUNCTION_TYPE")).append("\n");
			}
		}

	}

	/**
	 * Build generic prompt context for other catalog types
	 * 
	 * @param prompt
	 * @param llmPayload
	 */
	private void buildGenericPromptContext(StringBuilder prompt, Map<String, Object> llmPayload) {
		prompt.append("#### General Context\n");
		prompt.append(
				"- **Objective**: Based on the name and type, provide a specific, accurate description of its purpose and contents.\n");
	}

	/**
	 * Parse LLM response and extract JSON metadata
	 * 
	 * @param response
	 * @param targetFields
	 * @return
	 */
	private Map<String, Object> parseResponse(Object response, Set<String> targetFields) {
		if (response == null) {
			return buildFallbackMetadata(targetFields,
					"Metadata generation could not be completed because the model returned no response.");
		}

		if (response instanceof Map<?, ?>) {
			Map<String, Object> parsedResponse = (Map<String, Object>) response;
			return cleanParsedResponse(parsedResponse, targetFields);
		}

		try {
			String responseStr = response.toString().trim();
			if (responseStr.isEmpty()) {
				return buildFallbackMetadata(targetFields,
						"Metadata generation could not be completed because the model returned an empty response.");
			}

			// Try to extract JSON from response (handle markdown code blocks)
			int jsonStart = responseStr.indexOf("{");
			int jsonEnd = responseStr.lastIndexOf("}") + 1;

			if (jsonStart >= 0 && jsonEnd > jsonStart) {
				String jsonStr = responseStr.substring(jsonStart, jsonEnd);
				Map<String, Object> parsed = MAPPER.readValue(jsonStr, Map.class);
				return cleanParsedResponse(parsed, targetFields);
			}

			classLogger.warn("No JSON found in LLM response");
			return buildFallbackMetadata(targetFields, responseStr);

		} catch (Exception e) {
			classLogger.error("Failed to parse LLM response as JSON", e);
			return buildFallbackMetadata(targetFields, String.valueOf(response));
		}
	}

	/**
	 * 
	 * @param parsed
	 * @param targetFields
	 * @return
	 */
	private Map<String, Object> cleanParsedResponse(Map<String, Object> parsed, Set<String> targetFields) {
		Map<String, Object> cleaned = new LinkedHashMap<>();

		if (targetFields.contains("description")) {
			Object descObj = parsed.get("description");
			String desc = descObj == null ? "" : String.valueOf(descObj).trim();
			cleaned.put("description", desc);
		}

		if (targetFields.contains("tags")) {
			Object tagsObj = parsed.get("tags");
			List<String> tags = new ArrayList<>();

			if (tagsObj instanceof List<?>) {
				tags = ((List<?>) tagsObj).stream().filter(t -> t != null && !isEmpty(t.toString()))
						.map(Object::toString).map(String::trim).collect(Collectors.toList());
			}

			cleaned.put("tags", tags);
		}

		for (String field : targetFields) {
			if ("description".equals(field) || "tags".equals(field)) {
				continue;
			}

			Object value = parsed.get(field);
			cleaned.put(field, value == null ? "" : value);
		}

		if (parsed.containsKey("explanation")) {
			Object explanationObj = parsed.get("explanation");
			String explanation = explanationObj == null ? "" : String.valueOf(explanationObj).trim();
			if (!explanation.isEmpty()) {
				cleaned.put("explanation", explanation);
			}
		}

		return cleaned;
	}

	/**
	 * 
	 * @param targetFields
	 * @param fallbackDescription
	 * @return
	 */
	private Map<String, Object> buildFallbackMetadata(Set<String> targetFields, String fallbackDescription) {
		Map<String, Object> fallback = new LinkedHashMap<>();

		String message = fallbackDescription == null ? "" : fallbackDescription.trim();
		if (message.isEmpty()) {
			message = "Metadata generation could not produce structured JSON output.";
		}

		if (targetFields.contains("description")) {
			fallback.put("description", "");
		}

		if (targetFields.contains("tags")) {
			fallback.put("tags", new ArrayList<String>());
		}

		fallback.put("explanation", message);

		for (String field : targetFields) {
			if (!"description".equals(field) && !"tags".equals(field)) {
				fallback.put(field, "");
			}
		}

		return fallback;
	}

	/**
	 * 
	 * @param targetFields
	 * @return
	 */
	private Map<String, Object> buildResponseSchema(Set<String> targetFields) {
		Map<String, Object> properties = new LinkedHashMap<>();
		List<String> required = new ArrayList<>();

		for (String field : targetFields) {
			Map<String, Object> property = new LinkedHashMap<>();

			if ("description".equals(field)) {
				property.put("type", "string");
			} else if ("tags".equals(field)) {
				Map<String, Object> items = new LinkedHashMap<>();
				items.put("type", "string");
				property.put("type", "array");
				property.put("items", items);
			} else {
				property.put("type", "string");
			}

			properties.put(field, property);
			required.add(field);
		}

		Map<String, Object> explanationProperty = new LinkedHashMap<>();
		explanationProperty.put("type", "string");
		properties.put("explanation", explanationProperty);
		required.add("explanation");

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", required);
		schema.put("additionalProperties", false);

		Map<String, Object> jsonSchema = new LinkedHashMap<>();
		jsonSchema.put("name", "engine_metadata_output");
		jsonSchema.put("schema", schema);
		jsonSchema.put("strict", true);

		Map<String, Object> responseFormat = new LinkedHashMap<>();
		responseFormat.put("type", "json_schema");
		responseFormat.put("json_schema", jsonSchema);
		return responseFormat;
	}

	/**
	 * Build summary of what data was sent to LLM for transparency
	 * 
	 * @param llmPayload
	 * @return
	 */
	private Map<String, Object> buildDataSentSummary(Map<String, Object> llmPayload) {
		Map<String, Object> summary = new LinkedHashMap<>();
		summary.put("engineType", llmPayload.containsKey("engineType"));
		summary.put("schema", llmPayload.containsKey("schema"));
		summary.put("vectorFiles", llmPayload.containsKey("vectorFiles"));
		summary.put("vectorChunkSamples", llmPayload.containsKey("vectorChunkSamples"));
		summary.put("storageFiles", llmPayload.containsKey("storageFiles"));
		summary.put("storageFileSamples", llmPayload.containsKey("storageFileSamples"));
		summary.put("modelSmssInfo", llmPayload.containsKey("modelSmssInfo"));
		summary.put("funcSmssInfo", llmPayload.containsKey("funcSmssInfo"));
		summary.put("additionalContext", llmPayload.containsKey("additionalContext"));
		summary.put("existingDescription", llmPayload.containsKey("existingDescription"));
		return summary;
	}

	/**
	 * Helper method to get integer option with default
	 * 
	 * @param options
	 * @param key
	 * @param defaultVal
	 * @return
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
	 * 
	 * @param storage
	 * @param storagePath
	 * @param fileLimit
	 * @return
	 */
	private List<String> getStorageCandidateFilePaths(IStorageEngine storage, String storagePath, int fileLimit) {
		Set<String> candidatePaths = new LinkedHashSet<>();
		Set<String> visitedDirectories = new LinkedHashSet<>();
		Deque<String> pendingDirectories = new ArrayDeque<>();
		String rootPath = storagePath == null ? "/" : storagePath.trim().replace("\\", "/");
		if (rootPath.isEmpty()) {
			rootPath = "/";
		}
		if (!rootPath.startsWith("/")) {
			rootPath = "/" + rootPath;
		}
		while (rootPath.contains("//")) {
			rootPath = rootPath.replace("//", "/");
		}
		if (rootPath.length() > 1 && rootPath.endsWith("/")) {
			rootPath = rootPath.substring(0, rootPath.length() - 1);
		}
		pendingDirectories.add(rootPath);

		while (!pendingDirectories.isEmpty() && candidatePaths.size() < fileLimit) {
			String currentDirectory = pendingDirectories.poll();
			if (isEmpty(currentDirectory) || visitedDirectories.contains(currentDirectory)) {
				continue;
			}
			visitedDirectories.add(currentDirectory);

			List<Map<String, Object>> details;
			try {
				details = storage.listDetails(currentDirectory);
			} catch (Exception e) {
				classLogger.warn("Could not fetch storage details for metadata sampling at " + currentDirectory, e);
				continue;
			}

			if (details == null || details.isEmpty()) {
				continue;
			}

			for (Map<String, Object> detail : details) {
				if (candidatePaths.size() >= fileLimit) {
					break;
				}

				Object rawPath = detail.get("Path");
				if (rawPath == null) {
					continue;
				}

				String detailPath = rawPath.toString().trim().replace("\\", "/");
				if (detailPath.isEmpty()) {
					continue;
				}

				boolean isDirectory = Boolean.TRUE.equals(detail.get("IsDir"));

				while (detailPath.endsWith("/")) {
					detailPath = detailPath.substring(0, detailPath.length() - 1);
				}

				String fullPath;
				if (detailPath.startsWith("/")) {
					fullPath = detailPath;
				} else if ("/".equals(currentDirectory)) {
					fullPath = "/" + detailPath;
				} else {
					fullPath = currentDirectory + "/" + detailPath;
				}
				while (fullPath.contains("//")) {
					fullPath = fullPath.replace("//", "/");
				}

				if (isDirectory) {
					if (!visitedDirectories.contains(fullPath)) {
						pendingDirectories.add(fullPath);
					}
					continue;
				}

				if (isSupportedReadablePath(fullPath)) {
					candidatePaths.add(fullPath);
				}
			}
		}

		return new ArrayList<>(candidatePaths);
	}

	/**
	 * 
	 * @param path
	 * @return
	 */
	private boolean isSupportedReadablePath(String path) {
		if (path == null) {
			return false;
		}

		String normalized = path.trim().toLowerCase();
		if (normalized.isEmpty() || normalized.endsWith("/")) {
			return false;
		}

		return normalized.endsWith(".txt") || normalized.endsWith(".csv") || normalized.endsWith(".md")
				|| normalized.endsWith(".pdf") || normalized.endsWith(".doc") || normalized.endsWith(".docx");
	}

	/**
	 * 
	 * @param root
	 * @param files
	 * @param limit
	 */
	private void collectReadableFiles(File root, List<File> files, int limit) {
		if (root == null || files.size() >= limit || !root.exists()) {
			return;
		}

		File[] children = root.listFiles();
		if (children == null) {
			return;
		}

		for (File child : children) {
			if (files.size() >= limit) {
				return;
			}

			if (child.isDirectory()) {
				collectReadableFiles(child, files, limit);
				continue;
			}

			if (child.isFile() && isSupportedReadablePath(child.getName())) {
				files.add(child);
			}
		}
	}

	/**
	 * 
	 * @param root
	 * @param child
	 * @return
	 */
	private String getRelativePath(File root, File child) {
		try {
			return root.toPath().relativize(child.toPath()).toString().replace("\\", "/");
		} catch (Exception e) {
			return child.getName();
		}
	}

	/**
	 * Helper method to check if value is empty
	 * 
	 * @param value
	 * @return
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

	/**
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	private String readPdf(File file) throws IOException {
		try (PDDocument document = Loader.loadPDF(file)) {
			PDFTextStripper stripper = new PDFTextStripper();
			return stripper.getText(document);
		}
	}

	/**
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	private String readDocX(File file) throws IOException {
		try (FileInputStream fis = new FileInputStream(file); XWPFDocument doc = new XWPFDocument(fis)) {
			StringBuilder sb = new StringBuilder();
			for (XWPFParagraph p : doc.getParagraphs()) {
				sb.append(p.getText()).append("\n");
			}
			return sb.toString();
		}
	}

	/**
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	private String readDoc(File file) throws IOException {
		try (FileInputStream fis = new FileInputStream(file);
				HWPFDocument doc = new HWPFDocument(fis);
				WordExtractor extractor = new WordExtractor(doc)) {
			return extractor.getText();
		}
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
			return "List of metadata field names to generate, If not provided, defaults to ['description', 'tags'].";
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
					- storagePath (string):
					  Path in storage to inspect (default to /)
					- includeStorageFileNames (boolean):
					  Include storage file names.
					- storageFileNameLimit (number):
					  Maximum number of storage file names to include (default: 5).
					- includeStorageFileContent (boolean):
					  Copy files locally and read content snippets for context.
					- storageFileLimit (number):
					  Max number of files to read content from (default: 3).
					- storageCharLimit (number):
					  Max characters to read per file (default: 500).

					4) Models
					- includeModelSmssInfo (boolean):
					  Include model smss file content(model and model_type).

					5) Functions
					- includeFunctionSmssInfo (boolean):
					  Include function smss file content(function_type and function_description).

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
