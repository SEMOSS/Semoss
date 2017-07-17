package prerna.sablecc2.reactor.insights;

import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.util.Utility;

public class RunSavedInsightReactor extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
		// set the existing insight to the saved insight
		
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String engineName = getEngine();
		int rdbmsId = getRdbmsId();
		
		// this is kind of annoying
		// i need to get the engine to figure out what the recipe is
		// and then return a new insight object :/
		// and then merge those values with this existing insight
		IEngine engine = Utility.getEngine(engineName);
		List<Insight> in = engine.getInsight(rdbmsId + "");
		Insight insightWeWant = in.get(0);
		// set the values we care about
		this.insight.setEngineName(insightWeWant.getEngineName());
		this.insight.setRdbmsId(insightWeWant.getInsightId());
		this.insight.setInsightName(insightWeWant.getInsightName());
		this.insight.setPkslRecipe(insightWeWant.getPkslRecipe());
		this.insight.setOutput(insightWeWant.getOutput());
		
		// now re-run that output
		Map<String, Object> savedDataReceipe = this.insight.reRunPkslInsight();
		
		// return the data
		return new NounMetadata(savedDataReceipe, PkslDataTypes.CUSTOM_DATA_STRUCTURE, PkslOperationTypes.RUN_SAVED_INSIGHT_DATA);
	}

}
