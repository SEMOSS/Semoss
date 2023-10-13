package prerna.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetPanelCollectReactor extends AbstractInsightPanelReactor {
	
	public SetPanelCollectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.LIMIT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the limit
		int panelCollect = getPanelCollect();
		// merge the map options
		insightPanel.setNumCollect(panelCollect);
		
		Map<String, Object> retMap = new HashMap<>();
		retMap.put("panelId", insightPanel.getPanelId());
		retMap.put("numCollect", insightPanel.getNumCollect());
		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.PANEL_COLLECT);
	}
	
	private int getPanelCollect() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return ((Number) genericReactorGrs.get(0)).intValue();
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<Object> numNouns = this.curRow.getAllNumericColumns();
		if(numNouns != null && !numNouns.isEmpty()) {
			return ((Number) numNouns.get(0)).intValue();
		}
		
		throw new IllegalArgumentException("Must pass an integer value for the number to collect");
	}
	
}
