package prerna.engine.api;

import prerna.engine.impl.function.ImageDescriptionFunctionEngine;
import prerna.engine.impl.function.LocalPythonFunctionEngine;
import prerna.engine.impl.function.RESTFunctionEngine;
import prerna.engine.impl.function.aws.AWSComprehendFunctionEngine;
import prerna.engine.impl.function.aws.AWSTextractFunctionEngine;;

public enum FunctionTypeEnum {

	IMAGE_DESCRIPTION("IMAGE_DESCRIPTION", ImageDescriptionFunctionEngine.class.getName()),
	LOCAL_PYTHON("LOCAL_PYTHON", LocalPythonFunctionEngine.class.getName()),
	REST("REST", RESTFunctionEngine.class.getName()),

	AWS_COMPREHEND("AWS_COMPREHEND", AWSComprehendFunctionEngine.class.getName()),
	AWS_TEXTRACT("AWS_TEXTRACT", AWSTextractFunctionEngine.class.getName()),
	;

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
