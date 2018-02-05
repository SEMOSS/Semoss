package prerna.sablecc2.reactor.panel.rules;

import prerna.om.InsightPanel;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeletePanelColorByValueReactor extends AbstractPanelColorByValueReactor {
	
	public DeletePanelColorByValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.PANEL_COLOR_RULE_ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		String ruleId = getCbvId(1);
		QueryStruct2 rule = insightPanel.getColorByValue().remove(ruleId);
		boolean removedRule = true;
		if(rule == null) {
			removedRule = false;
		}
		
		return new NounMetadata(removedRule, PixelDataType.BOOLEAN, PixelOperationType.PANEL_COLOR_BY_VALUE);
	}
}