package prerna.sablecc2.reactor.insights;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class OpenEmptyInsightReactor extends AbstractInsightReactor {
	
	public OpenEmptyInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.PARAM_KEY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// create a new empty insight
		Insight newInsight = new Insight();
		newInsight.setCacheInWorkspace(true);
		InsightUtility.transferDefaultVars(this.insight, newInsight);
		InsightStore.getInstance().put(newInsight);
		InsightStore.getInstance().addToSessionHash(getSessionId(), newInsight.getInsightId());
		// set the user in the insight
		newInsight.setUser(this.insight.getUser());

		List<String> recipe = Arrays.asList(getRecipe());
		List<String> newRecipe = new Vector<String>();
		for(String r : recipe) {
			newRecipe.add(Utility.decodeURIComponent(r));
		}
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", newInsight.runPixel(newRecipe));
		runnerWraper.put("params", getExecutionParams());
		return new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.NEW_EMPTY_INSIGHT);
	}
	
}