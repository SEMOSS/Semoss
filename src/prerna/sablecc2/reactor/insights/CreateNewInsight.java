package prerna.sablecc2.reactor.insights;

import java.util.List;
import java.util.Map;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.util.Utility;

public class CreateNewInsight extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
		// set the existing insight to the saved insight
		
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String engineName = getEngine();
		int rdbmsId = getRdbmsId();
		
		// get the engine so i can get the new insight
		IEngine engine = Utility.getEngine(engineName);
		List<Insight> in = engine.getInsight(rdbmsId + "");
		Insight newInsight = in.get(0);
		// add the insight to the the store
		InsightStore.getInstance().put(newInsight);
		
		// now run the new insight
		Map<String, Object> savedDataReceipe = newInsight.reRunPkslInsight();
		
		// return the data
		return new NounMetadata(savedDataReceipe, PkslDataTypes.CUSTOM_DATA_STRUCTURE, PkslOperationTypes.NEW_INSIGHT_GENERATED);
	}

}
