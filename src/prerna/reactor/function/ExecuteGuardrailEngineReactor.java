package prerna.reactor.function;

public class ExecuteGuardrailEngineReactor extends ExecuteReactorFunctionEngineReactor {

	/*
	 * Just a convenience method
	 * Works the same as the reactor function engine
	 * Since guardrail engine is a reactor function engine as well
	 */
	
	@Override
	String getUnableToAccessError(String engineId) {
		return "Guardrail Engine " + engineId + " does not exist or user does not have access to this guardrail";
	}
}
