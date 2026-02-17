package prerna.reactor.workflow.engine.handlers;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.impl.MCPFactory;
import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;
import prerna.util.Utility;

/**
 * Executes an MCP tool on an engine and returns the result.
 * 
 * Config:
 *   engineId  — the engine/project ID that hosts the tool
 *   toolName  — the tool name to invoke
 *   params    — Map of parameters to pass to the tool
 */
public class RunToolStepHandler implements IWorkflowStepHandler {

	private static final Logger classLogger = LogManager.getLogger(RunToolStepHandler.class);

	@SuppressWarnings("unchecked")
	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();

		String engineId = (String) config.get("engineId");
		String toolName = (String) config.get("toolName");
		Map<String, Object> params = (Map<String, Object>) config.get("params");

		if (engineId == null || engineId.isEmpty()) {
			return StepResult.error(stepId, "RunTool step requires 'engineId' in config",
					System.currentTimeMillis() - start);
		}
		if (toolName == null || toolName.isEmpty()) {
			return StepResult.error(stepId, "RunTool step requires 'toolName' in config",
					System.currentTimeMillis() - start);
		}

		try {
			IEngine engine = Utility.getEngine(engineId);
			if (engine == null) {
				return StepResult.error(stepId, "Engine not found: " + engineId,
						System.currentTimeMillis() - start);
			}

			IMCP mcp = MCPFactory.build(engine);
			Object output = mcp.callTool(toolName, params, insight);

			return StepResult.success(stepId, output, System.currentTimeMillis() - start);
		} catch (Exception e) {
			classLogger.error("RunTool step '{}' failed", stepId, e);
			return StepResult.error(stepId, "Tool execution failed: " + e.getMessage(),
					System.currentTimeMillis() - start);
		}
	}
}
