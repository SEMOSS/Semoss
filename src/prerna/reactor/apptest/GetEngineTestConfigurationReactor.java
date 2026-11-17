package prerna.reactor.apptest;

import java.util.Map;
import java.util.LinkedHashMap;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.EngineTestConfiguration;

public class GetEngineTestConfigurationReactor extends AbstractAppTestReactor {
	public GetEngineTestConfigurationReactor() { keysToGet = new String[] { "project" }; keyRequired = new int[] { 1 }; }
	@Override public NounMetadata execute() {
		organizeKeys(); String project = targetProject();
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("target", project); out.put("configuration", EngineTestConfiguration.load(project));
		return new NounMetadata(out, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	@Override public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description("Read the explicit app's EngineTestConfiguration; configuration is null when none exists.")
				.key("project", "Target app project id").output(Map.class, "Target and optional engine test configuration");
	}
}
