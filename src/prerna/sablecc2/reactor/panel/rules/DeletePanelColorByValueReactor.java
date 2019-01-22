package prerna.sablecc2.reactor.panel.rules;

import java.util.HashMap;
import java.util.Map;

import prerna.om.InsightPanel;
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
		if(insightPanel == null) {
			throw new NullPointerException("Could not find insight panel");
		}
		String cbvRule = getCbvId(1);
		if(cbvRule == null) {
			throw new NullPointerException("Must provide the color by value name within the panel");
		}
		boolean removed = insightPanel.removeColorByValue(cbvRule);
		if(!removed) {
			throw new NullPointerException("Could not find the color by value rule within the panel");
		}
		// need to return
		// panelId
		// cbvRuleId (name)
		// filter info of the qs
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("panelId", insightPanel.getPanelId());
		retMap.put("name", cbvRule);
		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.REMOVE_PANEL_COLOR_BY_VALUE);
	}
}