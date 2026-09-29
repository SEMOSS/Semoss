package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.hooks.AgentHookRegistry;
import prerna.reactor.agent.runtime.PlatformAgentTools;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the deployment-level options an agent form needs before an agent
 * exists: the built-in tool catalog and the hook kinds the server recognizes.
 * These are the same {@code default_tools} and {@code known_hook_kinds} that
 * GetWorkspace returns for an existing agent.
 */
public class GetAgentFormOptionsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAgentFormOptionsReactor.class);

	public GetAgentFormOptionsReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> options = new HashMap<>();
		try {
			options.put("default_tools", PlatformAgentTools.getDefaultToolDefinitions());
		} catch (Exception e) {
			classLogger.warn("Failed to resolve default agent tools: {}", e.getMessage());
			options.put("default_tools", new ArrayList<>());
		}
		options.put("known_hook_kinds", new ArrayList<>(AgentHookRegistry.knownKinds()));
		return new NounMetadata(options, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Get the built-in agent tool catalog and known hook kinds for creating an agent";
	}
}
