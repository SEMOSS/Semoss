package prerna.reactor.apptest;

import java.util.Map;
import prerna.om.ThreadStore;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.EngineTestContexts;

public class EndAppTestSessionReactor extends AbstractAppTestReactor {
	public EndAppTestSessionReactor() { keysToGet = new String[] { "project", "sessionToken" }; keyRequired = new int[] { 1, 1 }; }
	@Override public NounMetadata execute() {
		organizeKeys(); String project = targetProject();
		EngineTestContexts.end(getString("sessionToken"), requireUser(), ThreadStore.getSessionId());
		return new NounMetadata(Map.of("target", project, "ended", true), PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	@Override public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("End the caller's app test database session. Its late requests/callbacks fail closed. Use the platform preview's Exit test database mode action to also restore normal browsing.")
				.key("project", "Target app project id").key("sessionToken", "Token returned by BeginAppTestSession")
				.output(Map.class, "Session end acknowledgement");
	}
}
