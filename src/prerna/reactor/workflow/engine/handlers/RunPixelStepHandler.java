package prerna.reactor.workflow.engine.handlers;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Executes a Pixel recipe string and returns its result.
 * 
 * Config:
 *   recipe — the Pixel expression to execute (e.g., "Query(engine=\"uuid\", query=\"SELECT ...\");")
 */
public class RunPixelStepHandler implements IWorkflowStepHandler {

	private static final Logger classLogger = LogManager.getLogger(RunPixelStepHandler.class);

	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();

		String recipe = (String) config.get("recipe");
		if (recipe == null || recipe.trim().isEmpty()) {
			return StepResult.error(stepId, "RunPixel step requires a 'recipe' in config",
					System.currentTimeMillis() - start);
		}

		try {
			PixelRunner runner = insight.runPixel(recipe);
			List<NounMetadata> results = runner.getResults();

			Object output = null;
			if (results != null && !results.isEmpty()) {
				NounMetadata lastResult = results.get(results.size() - 1);
				output = lastResult.getValue();
			}

			return StepResult.success(stepId, output, System.currentTimeMillis() - start);
		} catch (Exception e) {
			classLogger.error("RunPixel step '{}' failed", stepId, e);
			return StepResult.error(stepId, "Pixel execution failed: " + e.getMessage(),
					System.currentTimeMillis() - start);
		}
	}
}
