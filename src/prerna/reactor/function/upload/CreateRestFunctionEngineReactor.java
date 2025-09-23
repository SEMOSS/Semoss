package prerna.reactor.function.upload;

@Deprecated
public class CreateRestFunctionEngineReactor extends CreateFunctionEngineReactor {

	@Override
	public String getReactorDescription() {
		return """
				This reactor is deprecated and users should use the generic CreateFunctionEngine reactor.
				""";
	}
}
