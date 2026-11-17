package prerna.reactor.apptest;

import java.util.Map;
import prerna.om.ThreadStore;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.EngineTestConfiguration;
import prerna.testing.EngineTestContexts;

public class BeginAppTestSessionReactor extends AbstractAppTestReactor {
	public BeginAppTestSessionReactor() { keysToGet = new String[] { "project" }; keyRequired = new int[] { 1 }; }
	@Override public NounMetadata execute() {
		organizeKeys(); String project = targetProject();
		var context = EngineTestContexts.begin(requireUser(), project, ThreadStore.getSessionId(), EngineTestConfiguration.load(project));
		return new NounMetadata(Map.of("target", project, "sessionToken", context.token(), "configurationId", context.configurationId(),
				"expiresAt", context.summary().get("expiresAt")), PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	@Override public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Begin a caller/session/app-bound test database context before loading a recording preview. The platform browser bridge must propagate it; starting a context alone does not redirect an already loaded app. Expires after 30 minutes.")
				.key("project", "Target app project id").output(Map.class, "Session token, target, configuration id and expiry; do not persist the token in recordings");
	}
}
