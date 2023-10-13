package prerna.reactor.insights;

import java.util.List;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class ClearInsightReactor extends AbstractReactor {

	public ClearInsightReactor() {
		this.keysToGet = new String[] {"suppress", InsightUtility.OUTPUT_TYPE};
	}
	
	@Override
	public NounMetadata execute() {
		// first grab the FE state
		organizeKeys();
		boolean suppress = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[0]));
		String outputType = this.keyValue.get(this.keysToGet[1]);
		if(outputType == null) {
			outputType = InsightUtility.MAP_OUTPUT;
		}
		List<String> uiSteps = InsightUtility.getInsightUIStateSteps(this.insight, outputType);
		NounMetadata newNoun = InsightUtility.clearInsight(this.insight, suppress);
		newNoun.addAdditionalReturn(new NounMetadata(uiSteps, PixelDataType.CONST_STRING));
		return newNoun;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(this.keysToGet[0])) {
			return "Suppress the opType to be the default OPERATION";
		} else if(key.equals(InsightUtility.OUTPUT_TYPE)) {
			return "The value to return - as a 'string' or 'map'";
		}
		return super.getDescriptionForKey(key);
	}
}
