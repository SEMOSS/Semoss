package prerna.algorithm.api;

import prerna.om.Variable;

public interface ICodeExecution {

	String getExecutedCode();
	
	Variable.LANGUAGE getLanguage();
	
}
