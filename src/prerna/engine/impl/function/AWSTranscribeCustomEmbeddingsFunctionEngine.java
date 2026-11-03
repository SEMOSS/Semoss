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

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.util.Constants;

public class AWSTranscribeCustomEmbeddingsFunctionEngine extends AWSTranscribeFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTranscribeCustomEmbeddingsFunctionEngine.class);

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY,
				"AWS Transcribe Custom Embeddings - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY,
				"Execute AWS Transcribe for custom vector database embeddings");

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
		String folderPath = null;
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
		try {
			audioFileName = fileToProcess.getName();

			Insight insight = (Insight) parameters.get(Constants.INSIGHT);
			folderPath = this.objectPath + DIR_SEPARATOR + audioFileName;
			JSONObject transcriptionResult = getTranscriptionResultFromAudio(folderPath,
					fileToProcess.getAbsolutePath(), insight);
			JSONObject result = transcriptionResult.getJSONObject("results");
			JSONArray audioSegments = result.optJSONArray("audio_segments");
			if (audioSegments != null) {
				for (int i = 0; i < audioSegments.length(); i++) {
					JSONObject audioSegment = audioSegments.getJSONObject(i);
					writer.writeRow(audioFileName,
							audioSegment.getString("start_time") + " - " + audioSegment.getString("end_time"),
							audioSegment.getString("transcript"));
				}
			}

		} catch (Exception e) {
			classLogger.error("Failed to generate AWS Transcribe custom embeddings for file: " + audioFileName, e);
			throw new IllegalArgumentException(e);
		} finally {
			writer.close();
		}

		return writer.getRowsInCsv();
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TRANSCRIBE_CUSTOM_EMBEDDINGS.name();
	}

}
