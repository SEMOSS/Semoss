package prerna.reactor.function;

public class GetGuardrailEngineDefintionReactor extends GetFunctionEngineDefintionReactor {

	/*
	 * Just a convenience method
	 * Works the same as the function engine
	 * Since guardrail engine is a function engine as well
	 */
	
	@Override
	String getUnableToAccessError(String engineId) {
		return "Guardrail Engine " + engineId + " does not exist or user does not have access to this guardrail";
	}
}
