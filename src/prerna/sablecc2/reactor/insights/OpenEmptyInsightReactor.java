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

public class OpenEmptyInsightReactor extends AbstractInsightReactor {
	
	public OpenEmptyInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RECIPE.getKey(), ReactorKeysEnum.PARAM_KEY.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// create a new empty insight
		Insight newInsight = new Insight();
		InsightStore.getInstance().put(newInsight);
		// set the user in the insight
		newInsight.setUser(this.insight.getUser());
		
		List<String> recipe = Arrays.asList(getRecipe());
		List<String> newRecipe = new Vector<String>();
		for(String r : recipe) {
			newRecipe.add(Utility.decodeURIComponent(r));
		}
		Map<String, Object> insightMap = new HashMap<String, Object>();
		insightMap.put("insightData", newInsight.runPixel(newRecipe));
		insightMap.put("params", getExecutionParams());

		return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.NEW_EMPTY_INSIGHT);
	}
	
}