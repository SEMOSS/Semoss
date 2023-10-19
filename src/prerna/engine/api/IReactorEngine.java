package prerna.engine.api;

import java.util.List;

import prerna.engine.impl.function.FunctionParameter;
import prerna.reactor.IReactor;

public interface IReactorEngine extends IReactor, IEngine {

	
	
	// things to think about
	// how do use this as a reactor - given the lifecycle portion of it
	// may be one of the pieces is the id to get to it
	// it is almost I have to go from reactor -- engine -- reactor (this is really just the engine)
	// may be that is the answer the first reactor is just a wrapper which ultimately just passes everything to this guy
	// by setting the nounstore
	// the initial one is purely just a pass through
	
	
	String FUNCTION_TYPE = "FUNCTION_TYPE";
	
	String NAME_KEY = "FUNCTION_NAME";
	String DESCRIPTION_KEY = "FUNCTION_DESCRIPTION";
	String PARAMETER_KEY = "FUNCTION_PARAMETERS";
	String REQUIRED_PARAMETER_KEY = "FUNCTION_REQUIRED_PARAMETERS";


	org.json.JSONObject getFunctionDefintionJson();

	void setFunctionDescription(String functionDescription);
	
	String getFunctionDescription();
	
	// this should be reactor keys enum
	List<FunctionParameter> getParameters(); 

	
	List<String> getRequiredParameters();
	
	String getFunctionName();
}
