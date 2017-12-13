package prerna.sablecc2.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class SetPanelPositionReactor extends AbstractInsightPanelReactor {
	
	public SetPanelPositionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.PANEL_POSITION_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the ornaments that come as a map
		Map<String, Object> position = getPositionMapInput();
		// merge the map options
		insightPanel.setPosition(position);
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("panelId", insightPanel.getPanelId());
		retMap.put("positionMap", position);
		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_POSITION);
	}

	private Map<String, Object> getPositionMapInput() {
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (Map<String, Object>) panelNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
}
