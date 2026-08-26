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
package prerna.engine.api;

import prerna.engine.impl.function.AWSTextractCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.AWSTextractFunctionEngine;
import prerna.engine.impl.function.AWSTranscribeCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.AWSTranscribeFunctionEngine;
import prerna.engine.impl.function.AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine;
import prerna.engine.impl.function.BingSearchFunctionEngine;
import prerna.engine.impl.function.BraveSearchFunctionEngine;
import prerna.engine.impl.function.GoogleOCRCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.GoogleOCRFunctionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonCustomEmbeddingsFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.OpenAITranscribeFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;
import prerna.engine.impl.servicenow.ServiceNowFunctionEngine;

public enum FunctionTypeEnum {

	// normal function engines
	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonFunctionEngine.class.getName()),
	REST("REST", RESTFunctionEngine.class.getName()),

	BING_SEARCH("BING_SEARCH", BingSearchFunctionEngine.class.getName()),
	BRAVE_SEARCH("BRAVE_SEARCH", BraveSearchFunctionEngine.class.getName()),
	SERVICE_NOW("SERVICE_NOW", ServiceNowFunctionEngine.class.getName()),

	OPENAI_TRANSCRIBE("OPENAI_TRANSCRIBE", OpenAITranscribeFunctionEngine.class.getName()),
	AWS_TEXTRACT("AWS_TEXTRACT", AWSTextractFunctionEngine.class.getName()),
	AWS_TRANSCRIBE("AWS_TRANSCRIBE", AWSTranscribeFunctionEngine.class.getName()),
	GOOGLE_OCR("GOOGLE_OCR", GoogleOCRFunctionEngine.class.getName()),

	// special function engines for custom embeddings w/ vector databases
	AWS_TEXTRACT_CUSTOM_EMBEDDINGS("AWS_TEXTRACT_CUSTOM_EMBEDDINGS",
			AWSTextractCustomEmbeddingsFunctionEngine.class.getName()),
	AWS_TRANSCRIBE_CUSTOM_EMBEDDINGS("AWS_TRANSCRIBE_CUSTOM_EMBEDDINGS",
			AWSTranscribeCustomEmbeddingsFunctionEngine.class.getName()),
	AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS("AZURE_DOCUMENT_INTELLIGENCE_CUSTOM_EMBEDDINGS",
			AzureDocumentIntelligenceCustomEmbeddingsFuntionEngine.class.getName()),
	GOOGLE_OCR_CUSTOM_EMBEDDINGS("GOOGLE_OCR_CUSTOM_EMBEDDINGS",
			GoogleOCRCustomEmbeddingsFunctionEngine.class.getName()),
	IMAGE_DESCRIPTION("IMAGE_DESCRIPTION", ImageDescriptionFunctionEngine.class.getName()),
	LOCAL_PYTHON_CUSTOM_EMBEDDINGS("LOCAL_PYTHON_CUSTOM_EMBEDDINGS",
			LocalPythonCustomEmbeddingsFunctionEngine.class.getName());

	private String functionName;
	private String functionClass;

	FunctionTypeEnum(String functionName, String functionClass) {
		this.functionName = functionName;
		this.functionClass = functionClass;
	}

	public String getFunctionName() {
		return functionName;
	}

	public String getFunctionClass() {
		return functionClass;
	}

	/**
	 * @param name
	 * @return
	 */
	public static FunctionTypeEnum getEnumFromName(String name) {
		FunctionTypeEnum[] allValues = values();
		for (FunctionTypeEnum v : allValues) {
			if (v.getFunctionName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
