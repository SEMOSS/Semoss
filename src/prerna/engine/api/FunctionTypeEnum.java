package prerna.engine.api;

import prerna.engine.impl.function.AWSTextractFunctionEngine;
import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;
import prerna.engine.impl.function.AWSTranscribeFunctionEngine;

public enum FunctionTypeEnum {

	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonFunctionEngine.class.getName()),
	REST("REST", RESTFunctionEngine.class.getName()),
	AWS_TEXTRACT("AWS_TEXTRACT", AWSTextractFunctionEngine.class.getName()),
	IMAGE_DESCRIPTION("IMAGE_DESCRIPTION", ImageDescriptionFunctionEngine.class.getName()),
	AWS_Transcribe("AWS_Transcribe", AWSTranscribeFunctionEngine.class.getName());

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
