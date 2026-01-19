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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class AWSTextractCustomEmbeddingsFunctionEngine extends AWSTextractFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractCustomEmbeddingsFunctionEngine.class);

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY,
				"AWS Textract Custom Embeddings - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute AWS Textract");

		super.open(smssProp);
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		throw new IllegalArgumentException(
				"This function engine is only intended to be executed for custom vector db embeddings");
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		boolean pdf = fileToProcess.getName().toLowerCase().endsWith(".pdf");
		if (pdf) {
			try {
				return PDFUtility.pdfContainsImages(fileToProcess.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return false;
	}

	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		List<String> extractedTextFromDoc = new ArrayList<String>();
		String documentKeyName = fileToProcess.getName();
		String folderPath = null;
		String fileDir = null;
		Boolean saveFileToStorage = false;
		try (VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath)) {
			documentKeyName = fileToProcess.getName();
			folderPath = this.objectPath + DIR_SEPARATOR + documentKeyName;
			IStorageEngine storageeng = Utility.getStorage(this.storageEngineId);
			boolean pdf = documentKeyName.toLowerCase().endsWith(".pdf");
			saveFileToStorage = Boolean
					.parseBoolean(parameters.get(Constants.CUSTOM_DOCUMENT_PROCESSOR_USE_STORAGE).toString());
			if (pdf) {

				Insight insight = (Insight) parameters.get(Constants.INSIGHT);
				String insightId = insight.getInsightId();
				Insight in = InsightStore.getInstance().get(insightId);
				File instanceDir = new File(Utility.normalizePath(in.getInsightFolder()));

				fileDir = instanceDir + DIR_SEPARATOR + documentKeyName;
				File pdfFilePath = new File(fileDir);
				if (saveFileToStorage) {
					if (!SecurityEngineUtils.userCanEditEngine(insight.getUser(), this.storageEngineId)) {
						throw new IllegalArgumentException("Storage " + this.storageEngineId
								+ " does not exist or user does not have access to this engine");
					}
					Map<String, Object> metadata = new HashMap<>();
					metadata.put("utility", documentKeyName + "- Textract_functionality");
					storageeng.copyToStorage(fileDir,
							this.bucketName + DIR_SEPARATOR + this.objectPath + documentKeyName, metadata);
					extractedTextFromDoc = getAsyncTextExtraction(folderPath, this.bucketName);
					storageeng.deleteFromStorage(this.bucketName + DIR_SEPARATOR + this.objectPath + documentKeyName);
				} else {
					if (hasMoreThanPageLimits(pdfFilePath, this.pageLength)) {
						throw new IllegalArgumentException(
								"Unable to process the file because the total number of pages exceeds 5. "
										+ "The file is expected to be saved in storage before processing. "
										+ fileToProcess);
					} else {
						extractedTextFromDoc = getSyncTextExtraction(pdfFilePath);
					}
				}

			} else {
				throw new IllegalArgumentException(
						"Please provide valid input files using \"FILE_PATH\". File types supported include: pdf");
			}

			for (int i = 0; i < extractedTextFromDoc.size(); i++) {
				writer.writeRow(documentKeyName, String.valueOf(i + 1), extractedTextFromDoc.get(i));
			}

			return writer.getRowsInCsv();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TEXTRACT_CUSTOM_EMBEDDINGS.name();
	}
}