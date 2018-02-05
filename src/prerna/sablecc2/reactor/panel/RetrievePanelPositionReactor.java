package prerna.sablecc2.reactor.panel;

import java.util.HashMap;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RetrievePanelPositionReactor extends AbstractInsightPanelReactor {
	
	public RetrievePanelPositionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("panelId", insightPanel.getPanelId());
		retMap.put("positionMap", insightPanel.getPosition());
		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_POSITION);
	}

}
