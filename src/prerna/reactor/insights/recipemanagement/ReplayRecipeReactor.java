package prerna.reactor.insights.recipemanagement;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplayRecipeReactor extends AbstractReactor {

	private static final String CLASS_NAME = ReplayRecipeReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		// rerun the recipe
		logger.info("Re-executing the insight recipe... please wait as this operation may take some time");
		PixelRunner runner = this.insight.reRunPixelInsight(true);
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.RERUN_INSIGHT_RECIPE);
		return noun;
	}

}
