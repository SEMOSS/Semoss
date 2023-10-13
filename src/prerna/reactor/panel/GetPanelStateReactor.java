package prerna.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class GetPanelStateReactor extends AbstractInsightPanelReactor {

	/*
	 * This class is complimentary to SetPanelStateReactor
	 */
	
	public GetPanelStateReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), InsightUtility.OUTPUT_TYPE};
	}
	
	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		if(panel == null) {
			throw new NullPointerException("No panel was passed in to get the state");
		}
		String outputType = getOutput();
		
		// we will just serialize the insight panel
		return InsightUtility.getPanelState(panel, outputType);
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
