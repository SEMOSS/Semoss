package prerna.reactor.apptest;

import java.util.Map;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.EngineTestConfiguration;

/** Persist app-specific database mappings; normal app execution is unchanged. */
public class SaveEngineTestConfigurationReactor extends AbstractAppTestReactor {
	public SaveEngineTestConfigurationReactor() {
		keysToGet = new String[] { "project", "configuration" }; keyRequired = new int[] { 1, 1 };
	}
	@Override public NounMetadata execute() {
		organizeKeys();
		String project = targetProject();
		return new NounMetadata(EngineTestConfiguration.save(requireUser(), project, getMap("configuration")), PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	@Override public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Save EngineTestConfiguration for an explicit app. Maps real database IDs to separate local H2/SQLite test engines. Requires app edit access, real database view access and test database edit access. Does not modify app files or normal connections.")
				.key("project", "Target app project id")
				.key("configuration", k -> k.type(Map.class).description("name, optional id, and mappings: [{realEngineId,testEngineId}]. Unmapped app databases are blocked during test mode."))
				.output(Map.class, "Versioned engine test configuration");
	}
}
