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
package prerna.engine.impl.function;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.Utility;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;

public class AWSTranscribeCustomEmbeddingsFunctionEngine extends AWSTranscribeFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractCustomEmbeddingsFunctionEngine.class);

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY,
				"AWS Transcribe Custom Embeddings - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute Azure Document Intelligence");

		super.open(smssProp);
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		throw new IllegalArgumentException(
				"This function engine is only intended to be executed for custom vector db embeddings");
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		int lastDotIndex = fileToProcess.getName().lastIndexOf('.');
		if (lastDotIndex > 0 && lastDotIndex < fileToProcess.length() - 1) {
			String extension = fileToProcess.getName().substring(lastDotIndex + 1);
			return extension.equalsIgnoreCase("mp3") || extension.equalsIgnoreCase("wav")
					|| extension.equalsIgnoreCase("flac") || extension.equalsIgnoreCase("ogg")
					|| extension.equalsIgnoreCase("amr") || extension.equalsIgnoreCase("webm")
					|| extension.equalsIgnoreCase("mp4") || extension.equalsIgnoreCase("webm")
					|| extension.equalsIgnoreCase("mov") || extension.equalsIgnoreCase("avi");
		}
		return false;
	}

	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		String audioFileName = null;
		Object output = null;
		String folderPath = null;
		String fileDir = null;
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
		List<String> startAndEndTime = new ArrayList<>();
		List<String> extractedText = new ArrayList<>();
		Path tempFile = null;
		try {
			audioFileName = fileToProcess.getName();

			Insight insight = (Insight) parameters.get(Constants.INSIGHT);
			String insightId = insight.getInsightId();
			Insight in = InsightStore.getInstance().get(insightId);
			File instanceDir = new File(Utility.normalizePath(in.getInsightFolder()));

			fileDir = instanceDir + DIR_SEPARATOR + audioFileName;

			folderPath = this.objectPath + DIR_SEPARATOR + audioFileName;
			IStorageEngine storageEng = Utility.getStorage(this.storageEngineId);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("utility", audioFileName + "- Transcribe Custom Enbeddings Functionality");
			storageEng.copyToStorage(fileDir,
					this.bucketName + DIR_SEPARATOR + this.objectPath + fileToProcess.getName(), metadata);
			output = transcriptionTextFromAudio(folderPath);
			storageEng.deleteFromStorage(this.bucketName + DIR_SEPARATOR + this.objectPath + fileToProcess.getName());

			if (output == TranscriptionJobStatus.COMPLETED) {
				String filePathInBucket = this.objectPath + DIR_SEPARATOR + this.jobName + JSON_EXT;
				tempFile = Files.createTempFile("file-temp-", JSON_EXT);
				storageEng.copyToLocal(tempFile.toString(), filePathInBucket);

				StringBuilder stringBuilder = new StringBuilder();
				try (BufferedReader reader = Files.newBufferedReader(tempFile)) {
					String line;
					while ((line = reader.readLine()) != null) {
						stringBuilder.append(line);
					}
					JSONObject jsonobj = new JSONObject(stringBuilder.toString());
					JSONObject result = jsonobj.getJSONObject("results");
					JSONArray audioSegments = result.getJSONArray("audio_segments");
					if (audioSegments != null) {
						for (int i = 0; i < audioSegments.length(); i++) {
							startAndEndTime.add(audioSegments.getJSONObject(i).getString("start_time") + " - "
									+ audioSegments.getJSONObject(i).getString("end_time"));
							extractedText.add(audioSegments.getJSONObject(i).getString("transcript"));
						}
						for (int i = 0; i < extractedText.size(); i++) {
							writer.writeRow(audioFileName, startAndEndTime.get(i), extractedText.get(i));
						}
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if (tempFile != null) {
						try {
							Files.deleteIfExists(tempFile);
						} catch (IOException ioe) {
							classLogger.warn("Unable to delete temp file: " + tempFile, ioe);
						}
					}
					writer.close();
					try {
						storageEng.deleteFromStorage(
								this.bucketName + DIR_SEPARATOR + this.objectPath + DIR_SEPARATOR + this.jobName);
					} catch (Exception e) {
						classLogger.error("Failed to delete file from the storage: ", e);
					}
				}
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}

		return writer.getRowsInCsv();
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TRANSCRIBE_CUSTOM_EMBEDDINGS.name();
	}

}
