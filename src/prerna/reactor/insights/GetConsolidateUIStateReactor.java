package prerna.reactor.insights;

import java.util.List;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class GetConsolidateUIStateReactor extends AbstractReactor {

	public GetConsolidateUIStateReactor() {
		this.keysToGet = new String[] {InsightUtility.OUTPUT_TYPE};
	}
	
	@Override
	public NounMetadata execute() {
		String outputType = getOutput();
		List<String> pixelSteps = InsightUtility.getInsightUIStateSteps(this.insight, outputType);
		return new NounMetadata(pixelSteps, PixelDataType.VECTOR);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(InsightUtility.OUTPUT_TYPE)) {
			return "The value to return - as a 'string' or 'map'";
		}
		return super.getDescriptionForKey(key);
	}
	
	private String getOutput() {
		GenRowStruct grs = this.store.getNoun(InsightUtility.OUTPUT_TYPE);
		if(grs != null && !grs.isEmpty()) {
			String input = grs.get(0).toString();
			if(input.equalsIgnoreCase(InsightUtility.STRING_OUTPUT)) {
				return InsightUtility.STRING_OUTPUT;
			} else {
				return InsightUtility.MAP_OUTPUT;
			}
		}
		
		if(!this.curRow.isEmpty()) {
			if(this.curRow.toString().equalsIgnoreCase(InsightUtility.STRING_OUTPUT)) {
				return InsightUtility.STRING_OUTPUT;
			} else {
				return InsightUtility.MAP_OUTPUT;
			}
		}
		
		return InsightUtility.MAP_OUTPUT;
	}
	
}
