package prerna.sablecc2.reactor.panel.rules;

import prerna.om.InsightPanel;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class RetrievePanelColorByValueReactor extends AbstractPanelColorByValueReactor {
	
	public RetrievePanelColorByValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.PANEL_COLOR_RULE_ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		QueryStruct2 qs = insightPanel.getColorByValue().get(getCbvId(1));
		// make a copy
		// so that the original QS is not modified if additional changes
		// are added to the rule
		QueryStruct2 newQs = new QueryStruct2();
		newQs.merge(qs);
		return new NounMetadata(newQs, PixelDataType.QUERY_STRUCT, PixelOperationType.QUERY);
	}
}