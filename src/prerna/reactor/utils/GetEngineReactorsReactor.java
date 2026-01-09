package prerna.reactor.utils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorFactory;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetEngineReactorsReactor extends AbstractReactor {

	private static final String MCP_TOOLS_NAME_JMES = "tools[].name";

	public GetEngineReactorsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Engine id is required");
		}

		IEngine engine = Utility.getEngine(engineId);
		IEngine.CATALOG_TYPE catalogType = engine.getCatalogType();

		String basePackage;
		switch (catalogType) {
		case DATABASE:
			basePackage = "prerna.reactor.qs";
			break;
		case STORAGE:
			basePackage = "prerna.reactor.storage";
			break;
		case MODEL:
			basePackage = "prerna.reactor.model";
			break;
		case VECTOR:
			basePackage = "prerna.reactor.vector";
			break;
		case FUNCTION:
			basePackage = "prerna.reactor.function";
			break;
		case GUARDRAIL:
			basePackage = "prerna.reactor.guardrail.upload";
			break;
		default:
			throw new IllegalArgumentException("Unsupported catalog type: " + catalogType);
		}

		// get generated MCP tool names
		Set<String> generatedReactorNames = getMCPGeneratedToolNames(engine, engineId, MCP_TOOLS_NAME_JMES);

		// load reactors info from the engine package
		List<Map<String, Object>> reactorsInfo = ReactorFactory.getEngineReactorsFromBasePackage(basePackage,
				generatedReactorNames);

		return new NounMetadata(reactorsInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);

	}

	/**
	 * 
	 * @param engine
	 * @param engineId
	 * @param jmesExpression
	 * @return
	 */
	private Set<String> getMCPGeneratedToolNames(IEngine engine, String engineId, String jmesExpression) {

		String mcpPath = MCPUtility.getPixelMcpPath(engine, engineId);
		JsonNode node = MCPUtility.executeMcpJmes(mcpPath, jmesExpression);

		Set<String> toolNames = new HashSet<>();

		if (node != null && node.isArray()) {
			for (JsonNode n : node) {
				String name = n.asText(null);
				if (name != null && !name.isEmpty()) {
					toolNames.add(name);
				}
			}
		}
		return toolNames;
	}

	@Override
	public String getReactorDescription() {
		return "Returns a list of engine-specific reactors and marks each reactor with its MCP generation state based on existing MCP configuration files.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Engine identifier used to fetch associated reactors.";
		}
		return super.getDescriptionForKey(key);
	}

}
