package prerna.reactor.panel;

import java.util.HashMap;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPanelCollectReactor extends AbstractInsightPanelReactor {
	
	public GetPanelCollectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("panelId", insightPanel.getPanelId());
		retMap.put("numCollect", insightPanel.getNumCollect());
		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.PANEL_COLLECT);
	}
	
}
