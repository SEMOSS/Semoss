package prerna.engine.api;

import prerna.engine.impl.function.AWSTextractFunctionEngine;
import prerna.engine.impl.function.AzureSpeechToTextFunctionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;

public enum FunctionTypeEnum {

	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonFunctionEngine.class.getName()),
	REST("REST", RESTFunctionEngine.class.getName()),
	AWS_TEXTRACT("AWS_TEXTRACT", AWSTextractFunctionEngine.class.getName()),
	AZURE_SPEECH_TO_TEXT("AZURE SPEECH TO TEXT", AzureSpeechToTextFunctionEngine.class.getName()),
	IMAGE_DESCRIPTION("IMAGE_DESCRIPTION", ImageDescriptionFunctionEngine.class.getName());

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
	 * 
	 * @param name
	 * @return
	 */
	public static FunctionTypeEnum getEnumFromName(String name) {
		FunctionTypeEnum[] allValues = values();
		for(FunctionTypeEnum v : allValues) {
			if(v.getFunctionName().equalsIgnoreCase(name)) {
				return v;
			}
		}
		throw new IllegalArgumentException("Invalid input for name " + name);
	}
}
