package prerna.sablecc2.reactor.panel.rules;

import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeletePanelColorByValueReactor extends AbstractPanelColorByValueReactor {
	
	public DeletePanelColorByValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.NAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		String ruleId = getCbvId(1);
		SelectQueryStruct rule = insightPanel.getColorByValue().remove(ruleId);
		boolean removedRule = true;
		if(rule == null) {
			removedRule = false;
		}
		
		return new NounMetadata(removedRule, PixelDataType.BOOLEAN, PixelOperationType.REMOVE_PANEL_COLOR_BY_VALUE);
	}
}