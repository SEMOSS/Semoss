package prerna.reactor.apptest;

import java.util.Map;
import java.nio.file.Files;
import java.io.IOException;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.EngineTestConfiguration;

public class DeleteEngineTestConfigurationReactor extends AbstractAppTestReactor {
	public DeleteEngineTestConfigurationReactor() { keysToGet = new String[] { "project" }; keyRequired = new int[] { 1 }; }
	@Override public NounMetadata execute() {
		organizeKeys(); String project = targetProject();
		try { Files.deleteIfExists(EngineTestConfiguration.file(project)); }
		catch (IOException e) { throw new IllegalArgumentException("Unable to remove engine test configuration", e); }
		return new NounMetadata(Map.of("target", project, "deleted", true), PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	@Override public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Remove an app's test database configuration only when requested. Does not delete databases. Recordings referencing the removed configuration fail rather than fall back to production.")
				.key("project", "Target app project id").output(Map.class, "Target and deletion acknowledgement");
	}
}
