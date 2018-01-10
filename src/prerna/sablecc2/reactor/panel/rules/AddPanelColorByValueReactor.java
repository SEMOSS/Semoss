package prerna.sablecc2.reactor.panel.rules;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class AddPanelColorByValueReactor extends AbstractPanelColorByValueReactor {
	
	public AddPanelColorByValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), 
				ReactorKeysEnum.PANEL_COLOR_RULE_ID.getKey(),
				ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		String cbvRule = getCbvId(1);
		QueryStruct2 qs = getQs();
		insightPanel.getColorByValue().put(cbvRule, qs);
		return new NounMetadata(qs, PixelDataType.QUERY_STRUCT, PixelOperationType.QUERY);
	}
	
	private QueryStruct2 getQs() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[2]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (QueryStruct2) genericReactorGrs.get(0);
		}
		
		genericReactorGrs = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (QueryStruct2) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return (QueryStruct2) strNouns.get(0).getValue();
		}
				
		// well, you are out of luck
		throw new IllegalArgumentException("Need to specify a query struct for the rule");
	}
}