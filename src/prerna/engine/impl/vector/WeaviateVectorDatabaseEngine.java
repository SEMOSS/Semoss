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
package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.weaviate.client6.v1.api.Authentication;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.CollectionHandle;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.Tokenization;
import io.weaviate.client6.v1.api.collections.Vectors;
import io.weaviate.client6.v1.api.collections.WeaviateObject;
import io.weaviate.client6.v1.api.collections.aggregate.AggregateResponseGroup;
import io.weaviate.client6.v1.api.collections.aggregate.AggregateResponseGrouped;
import io.weaviate.client6.v1.api.collections.aggregate.GroupBy;
import io.weaviate.client6.v1.api.collections.aggregate.GroupedBy;
import io.weaviate.client6.v1.api.collections.data.DeleteManyResponse;
import io.weaviate.client6.v1.api.collections.data.InsertManyResponse;
import io.weaviate.client6.v1.api.collections.query.Filter;
import io.weaviate.client6.v1.api.collections.query.Metadata;
import io.weaviate.client6.v1.api.collections.query.NearVector;
import io.weaviate.client6.v1.api.collections.query.QueryMetadata;
import io.weaviate.client6.v1.api.collections.query.QueryResponse;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.util.Constants;
import prerna.util.Utility;

public class WeaviateVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(WeaviateVectorDatabaseEngine.class);

	public static final String WEAVIATE_CLASSNAME = "WEAVIATE_CLASSNAME";
	public static final String AUTOCUT = "AUTOCUT";
	// v6 speaks gRPC in addition to REST. The gRPC endpoint defaults to the REST
	// host on port 50051 (Weaviate's default) unless overridden in the .smss.
	public static final String WEAVIATE_HTTP_PORT = "WEAVIATE_HTTP_PORT";
	public static final String WEAVIATE_GRPC_HOST = "WEAVIATE_GRPC_HOST";
	public static final String WEAVIATE_GRPC_PORT = "WEAVIATE_GRPC_PORT";

	/**
	 * SMSS key: alpha for hybrid fusion - 1.0 = pure vector, 0.0 = pure keyword,
	 * 0.5 = balanced. Default 0.5.
	 */
	public static final String HYBRID_ALPHA = "HYBRID_ALPHA";

	private static final float DEFAULT_HYBRID_ALPHA = 0.5f;
	private static final int DEFAULT_GRPC_PORT = 50051;

	// upper bound on the number of distinct documents returned by listDocuments
	private static final int LIST_DOCUMENTS_GROUP_LIMIT = 10000;

	// property names as stored in Weaviate - Weaviate lowercases the first letter
	// of property names, so we derive them from the shared column constants
	private static final String PROP_SOURCE = VectorDatabaseCSVTable.SOURCE.toLowerCase();
	private static final String PROP_MODALITY = VectorDatabaseCSVTable.MODALITY.toLowerCase();
	private static final String PROP_DIVIDER = VectorDatabaseCSVTable.DIVIDER.toLowerCase();
	private static final String PROP_PART = VectorDatabaseCSVTable.PART.toLowerCase();
	private static final String PROP_TOKENS = VectorDatabaseCSVTable.TOKENS.toLowerCase();
	private static final String PROP_CONTENT = VectorDatabaseCSVTable.CONTENT.toLowerCase();

	private WeaviateClient client = null;
	private String host = null;
	private String protocol = "https";
	private String apiKey = null;
	private int httpPort = -1;
	private String grpcHost = null;
	private int grpcPort = DEFAULT_GRPC_PORT;

	private String className = null;
	private int autocut = 1;
	private float hybridAlpha = DEFAULT_HYBRID_ALPHA;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.host = smssProp.getProperty(Constants.HOSTNAME);
		if (this.host == null || (this.host = this.host.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the host");
		}

		if (this.host.startsWith("https://")) {
			this.host = this.host.substring("https://".length(), host.length());
		} else if (this.host.startsWith("http://")) {
			this.protocol = "http";
			this.host = this.host.substring("http://".length(), host.length());
		}

		// allow the host to carry an explicit port (host:port)
		int portSeparator = this.host.indexOf(':');
		if (portSeparator > -1) {
			try {
				this.httpPort = Integer.parseInt(this.host.substring(portSeparator + 1).trim());
			} catch (NumberFormatException e) {
				classLogger.error("Invalid port in host '{}'", this.host, e);
			}
			this.host = this.host.substring(0, portSeparator);
		}

		String httpPortStr = smssProp.getProperty(WEAVIATE_HTTP_PORT);
		if (httpPortStr != null && !(httpPortStr = httpPortStr.trim()).isEmpty()) {
			try {
				this.httpPort = Integer.parseInt(httpPortStr);
			} catch (NumberFormatException e) {
				classLogger.error("Invalid input for {} '{}'. Must be an integer value", WEAVIATE_HTTP_PORT,
						httpPortStr, e);
			}
		}
		if (this.httpPort < 0) {
			this.httpPort = "https".equals(this.protocol) ? 443 : 80;
		}

		this.grpcHost = smssProp.getProperty(WEAVIATE_GRPC_HOST);
		if (this.grpcHost == null || (this.grpcHost = this.grpcHost.trim()).isEmpty()) {
			this.grpcHost = this.host;
		}

		String grpcPortStr = smssProp.getProperty(WEAVIATE_GRPC_PORT);
		if (grpcPortStr != null && !(grpcPortStr = grpcPortStr.trim()).isEmpty()) {
			try {
				this.grpcPort = Integer.parseInt(grpcPortStr);
			} catch (NumberFormatException e) {
				classLogger.error("Invalid input for {} '{}'. Must be an integer value", WEAVIATE_GRPC_PORT,
						grpcPortStr, e);
			}
		}

		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		if (this.apiKey == null || (this.apiKey = this.apiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must define the api key");
		}

		this.className = smssProp.getProperty(WEAVIATE_CLASSNAME);

		connect2Weaviate();
		createClass(this.className);

		String autoCutStr = smssProp.getProperty(AUTOCUT);
		if (autoCutStr != null && !(autoCutStr = autoCutStr.trim()).isEmpty()) {
			try {
				this.autocut = Integer.parseInt(autoCutStr);
			} catch (NumberFormatException e) {
				classLogger.error("Invalid input for autocut '{}'. Must be a positive integer value", autoCutStr, e);
				throw new IllegalArgumentException(
						"Invalid input for autocut. Must be a positive integer value. Value was: " + autoCutStr);
			}
		}

		if (this.useHybridSearch && smssProp.containsKey(HYBRID_ALPHA)) {
			try {
				float parsed = Float.parseFloat(smssProp.getProperty(HYBRID_ALPHA));
				if (parsed >= 0.0f && parsed <= 1.0f) {
					this.hybridAlpha = parsed;
				} else {
					classLogger.warn("HYBRID_ALPHA '{}' must be between 0.0 and 1.0; defaulting to {}", parsed,
							DEFAULT_HYBRID_ALPHA);
				}
			} catch (NumberFormatException e) {
				classLogger.warn("HYBRID_ALPHA '{}' is not a valid number; defaulting to {}",
						smssProp.getProperty(HYBRID_ALPHA), DEFAULT_HYBRID_ALPHA);
			}
		}
	}

	@Override
	protected String getDefaultDistanceMethod() {
		return "Cosine Similarity";
	}

	/**
	 * Establishes the v6 client connection using both the REST and gRPC endpoints.
	 */
	private void connect2Weaviate() {
		final String scheme = this.protocol;
		final String httpHost = this.host;
		final int httpPortValue = this.httpPort;
		final String grpcHostValue = this.grpcHost;
		final int grpcPortValue = this.grpcPort;
		final String key = this.apiKey;

		this.client = WeaviateClient
				.connectToCustom(conn -> conn.scheme(scheme).httpHost(httpHost).httpPort(httpPortValue)
						.grpcHost(grpcHostValue).grpcPort(grpcPortValue).authentication(Authentication.apiKey(key)));
	}

	/**
	 * Creates the collection if it does not already exist. Vectors are supplied by
	 * SEMOSS (no server-side vectorizer). The metadata properties are defined with
	 * FIELD tokenization so equality filters match whole values (e.g. a full
	 * filename); content keeps the default WORD tokenization so BM25/hybrid search
	 * works. Any properties not defined here (e.g. tokens) are auto-schemad on
	 * insert.
	 *
	 * @param className
	 */
	private void createClass(String className) throws IOException {
		if (className == null) {
			throw new IllegalArgumentException("Must define " + WEAVIATE_CLASSNAME);
		}
		if (!client.collections.exists(className)) {
			client.collections.create(className,
					c -> c.properties(Property.text(PROP_SOURCE, p -> p.tokenization(Tokenization.FIELD)),
							Property.text(PROP_MODALITY, p -> p.tokenization(Tokenization.FIELD)),
							Property.text(PROP_DIVIDER, p -> p.tokenization(Tokenization.FIELD)),
							Property.text(PROP_PART, p -> p.tokenization(Tokenization.FIELD)),
							Property.text(PROP_CONTENT)));
		}
	}

	@Override
	public List<FileEmbeddingStatus> addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight,
			Map<String, Object> parameters) throws Exception {
		if (!modelPropsLoaded) {
			verifyModelProps();
		}

		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}

		IModelEngine embeddingsEngine = Utility.getModel(this.embedderEngineId);
		// send all the strings to embed in one shot
		try {
			vectorCsvTable.generateAndAssignEmbeddings(embeddingsEngine, insight);
		} catch (Exception e) {
			classLogger.error("Error occurred creating the embeddings for the generated chunks", e);
			throw new IllegalArgumentException(
					"Error occurred creating the embeddings for the generated chunks. Detailed error message = "
							+ e.getMessage());
		}

		CollectionHandle<Map<String, Object>> collection = client.collections.use(className);

		// Track row counts per source
		Map<String, Integer> fileRecordCountMap = new HashMap<>();
		Map<String, Integer> successCountMap = new HashMap<>();
		Map<String, Integer> failedCountMap = new HashMap<>();

		// build the objects to insert while keeping a parallel list of their sources so
		// we can map per-object results back to files
		List<WeaviateObject<Map<String, Object>>> objects = new ArrayList<>();
		List<String> rowSources = new ArrayList<>();
		for (int rowIndex = 0; rowIndex < vectorCsvTable.rows.size(); rowIndex++) {
			VectorDatabaseCSVRow row = vectorCsvTable.getRows().get(rowIndex);
			String source = row.getSource();
			fileRecordCountMap.put(source, fileRecordCountMap.getOrDefault(source, 0) + 1);
			try {
				Map<String, Object> properties = new HashMap<>();
				properties.put(PROP_SOURCE, row.getSource());
				properties.put(PROP_MODALITY, row.getModality());
				properties.put(PROP_DIVIDER, row.getDivider());
				properties.put(PROP_PART, row.getPart());
				properties.put(PROP_TOKENS, row.getTokens());
				properties.put(PROP_CONTENT, row.getContent());

				float[] vector = toFloatArray(row.getEmbeddings());

				objects.add(WeaviateObject
						.<Map<String, Object>>of(obj -> obj.properties(properties).vectors(Vectors.of(vector))));
				rowSources.add(source);
			} catch (Exception ex) {
				classLogger.error("Failed to process embedding row for source '{}'", source, ex);
				failedCountMap.put(source, failedCountMap.getOrDefault(source, 0) + 1);
			}
		}

		try {
			InsertManyResponse response = collection.data.insertMany(objects);
			List<InsertManyResponse.InsertObject> results = response.responses();
			for (int i = 0; i < rowSources.size(); i++) {
				String source = rowSources.get(i);
				boolean successful = i < results.size() && results.get(i).successful();
				if (successful) {
					successCountMap.put(source, successCountMap.getOrDefault(source, 0) + 1);
				} else {
					failedCountMap.put(source, failedCountMap.getOrDefault(source, 0) + 1);
				}
			}
		} catch (Exception e) {
			classLogger.error("Weaviate batch insert failed", e);
			// everything we attempted to insert is a failure
			for (String source : rowSources) {
				failedCountMap.put(source, failedCountMap.getOrDefault(source, 0) + 1);
			}
		}

		List<FileEmbeddingStatus> fileStatusList = new ArrayList<>();
		for (String source : fileRecordCountMap.keySet()) {
			int total = fileRecordCountMap.getOrDefault(source, 0);
			int success = successCountMap.getOrDefault(source, 0);
			int failed = failedCountMap.getOrDefault(source, 0);

			String status;
			if (success == total) {
				status = "SUCCESS";
			} else if (success > 0 && success < total) {
				status = "PARTIAL";
			} else {
				status = "FAILED";
			}
			fileStatusList.add(new FileEmbeddingStatus(source, status, success, failed, total));
		}

		return fileStatusList;
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws IOException {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		List<String> sourceNames = new ArrayList<>();
		for (String document : fileNames) {
			String documentName = FilenameUtils.getName(document);
			File f = new File(document);
			if (f.exists() && f.getName().endsWith(".csv")) {
				sourceNames.addAll(VectorDatabaseCSVTable.pullSourceColumn(f));
			} else {
				sourceNames.add(documentName);
			}
		}

		CollectionHandle<Map<String, Object>> collection = client.collections.use(className);

		List<String> filesToRemoveFromCloud = new ArrayList<>();
		// delete each source's records and then remove the physical document
		for (String sourceName : sourceNames) {
			DeleteManyResponse result = collection.data.deleteMany(Filter.property(PROP_SOURCE).eq(sourceName));
			classLogger.info("Deleted file '{}' <> matched {} deleted {}", sourceName, result.matches(),
					result.successful());

			String documentName = Paths.get(sourceName).getFileName().toString();
			// remove the physical documents
			File documentFile = new File(
					this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + "documents",
					documentName);
			try {
				if (documentFile.exists()) {
					FileUtils.forceDelete(documentFile);
					filesToRemoveFromCloud.add(documentFile.getAbsolutePath());
				}
			} catch (IOException e) {
				classLogger.error("Failed to delete document file '{}'", documentFile.getAbsolutePath(), e);
			}
		}

		if (ClusterUtil.IS_CLUSTER) {
			Thread deleteFilesFromCloudThread = new Thread(new DeleteFilesFromEngineRunner(engineId,
					this.getCatalogType(), filesToRemoveFromCloud.stream().toArray(String[]::new)));
			deleteFilesFromCloudThread.start();
		}
	}

	@Override
	public List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit,
			Map<String, Object> parameters) {
		if (insight == null) {
			throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		}
		if (limit == null) {
			limit = 3;
		}
		int cutter = autocut;
		if (parameters.containsKey(AUTOCUT)) {
			cutter = Integer.parseInt(String.valueOf(parameters.get(AUTOCUT)));
		}
		// captured by the query lambda below, so it must be effectively final
		final int autolimit = cutter;

		List<Map<String, Object>> retOut = new ArrayList<>();

		float[] vector = toFloatArray(getEmbeddingsFloat(searchStatement, insight));
		int resultLimit = limit.intValue();

		// optional content + metadata filters translated into a single weaviate filter
		final Filter requestFilter = buildRequestFilter(parameters);

		CollectionHandle<Map<String, Object>> collection = client.collections.use(className);

		QueryResponse<Map<String, Object>> response;
		if (this.useHybridSearch) {
			// hybrid path: Weaviate fuses BM25 + vector server-side and returns a single
			// fused score
			response = collection.query.hybrid(searchStatement, h -> {
				var builder = h.nearVector(NearVector.of(vector)).alpha(this.hybridAlpha).limit(resultLimit)
						.returnMetadata(Metadata.SCORE);
				if (requestFilter != null) {
					builder = builder.filters(requestFilter);
				}
				return builder;
			});
		} else {
			response = collection.query.nearVector(vector, nv -> {
				var builder = nv.autolimit(autolimit).limit(resultLimit).returnMetadata(Metadata.CERTAINTY,
						Metadata.DISTANCE);
				if (requestFilter != null) {
					builder = builder.filters(requestFilter);
				}
				return builder;
			});
		}

		for (WeaviateObject<Map<String, Object>> object : response.objects()) {
			Map<String, Object> properties = object.properties();
			QueryMetadata metadata = object.queryMetadata();

			Map<String, Object> outputMap = new HashMap<>();
			outputMap.put(VectorDatabaseCSVTable.SOURCE, properties.get(PROP_SOURCE));
			outputMap.put(VectorDatabaseCSVTable.DIVIDER, properties.get(PROP_DIVIDER));
			outputMap.put(VectorDatabaseCSVTable.MODALITY, properties.get(PROP_MODALITY));
			outputMap.put(VectorDatabaseCSVTable.PART, properties.get(PROP_PART));
			outputMap.put(VectorDatabaseCSVTable.CONTENT, properties.get(PROP_CONTENT));
			if (this.useHybridSearch) {
				outputMap.put("Score", metadata == null ? null : metadata.score());
			} else {
				outputMap.put("Score", metadata == null ? null : metadata.certainty());
				outputMap.put("Distance", metadata == null ? null : metadata.distance());
			}
			retOut.add(outputMap);
		}
		return retOut;
	}

	/**
	 * Translates the content ({@link #FILTERS_KEY}) and metadata
	 * ({@link #METADATA_FILTERS_KEY}) filters from the request parameters into a
	 * single Weaviate filter, AND'ing them together.
	 *
	 * @param parameters the nearest neighbor request parameters
	 * @return the combined filter, or null when no filters were provided
	 */
	@SuppressWarnings("unchecked")
	private Filter buildRequestFilter(Map<String, Object> parameters) {
		List<IQueryFilter> combinedFilters = new ArrayList<>();
		Object contentFilters = parameters.get(FILTERS_KEY);
		if (contentFilters instanceof List) {
			combinedFilters.addAll((List<IQueryFilter>) contentFilters);
		}
		Object metadataFilters = parameters.get(METADATA_FILTERS_KEY);
		if (metadataFilters instanceof List) {
			combinedFilters.addAll((List<IQueryFilter>) metadataFilters);
		}
		return WeaviateVectorQueryFilterTranslationHelper.translate(combinedFilters);
	}

	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}

		// pull the distinct Source values from weaviate by grouping on the source
		// property, then enrich each with size/last modified from the filesystem
		CollectionHandle<Map<String, Object>> collection = client.collections.use(className);
		AggregateResponseGrouped grouped = collection.aggregate
				.overAll(GroupBy.property(PROP_SOURCE, b -> b.limit(LIST_DOCUMENTS_GROUP_LIMIT)));

		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR
				+ DOCUMENTS_FOLDER_NAME);

		List<Map<String, Object>> fileList = new ArrayList<>();
		for (AggregateResponseGroup<?> group : grouped.groups()) {
			GroupedBy<?> groupedBy = group.groupedBy();
			if (!groupedBy.isText() || groupedBy.text() == null) {
				continue;
			}
			String fileName = groupedBy.text();

			Map<String, Object> fileInfo = new HashMap<>();
			fileInfo.put("fileName", fileName);

			File thisF = new File(documentsDir, fileName);
			if (thisF.exists() && thisF.isFile()) {
				long fileSizeInBytes = thisF.length();
				double fileSizeInMB = (double) fileSizeInBytes / (1024);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String lastModified = dateFormat.format(new Date(thisF.lastModified()));

				// add file size and last modified into the map
				fileInfo.put("fileSize", fileSizeInMB);
				fileInfo.put("lastModified", lastModified);
			}
			fileList.add(fileInfo);
		}

		return fileList;
	}

	@Override
	public List<Map<String, Object>> listAllRecords(Map<String, Object> parameters) {
		throw new IllegalArgumentException("This method has not been implemented yet");
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.WEAVIATE;
	}

	/**
	 * Converts boxed embedding values into the primitive {@code float[]} that the
	 * v6 client expects.
	 */
	private static float[] toFloatArray(List<? extends Number> embedding) {
		float[] vector = new float[embedding.size()];
		for (int i = 0; i < vector.length; i++) {
			vector[i] = embedding.get(i).floatValue();
		}
		return vector;
	}

	private static float[] toFloatArray(Float[] embedding) {
		float[] vector = new float[embedding.length];
		for (int i = 0; i < vector.length; i++) {
			vector[i] = embedding[i];
		}
		return vector;
	}

}
