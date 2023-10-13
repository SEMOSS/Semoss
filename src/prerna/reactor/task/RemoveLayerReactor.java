package prerna.reactor.task;

import java.util.HashMap;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveLayerReactor extends AbstractReactor {
	
	public RemoveLayerReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.LAYER.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get the task id
		String panelId = this.keyValue.get(this.keysToGet[0]);
		String layerId = this.keyValue.get(this.keysToGet[1]);
		
		Map<String, String> removeLayerMap = new HashMap<String, String>();
		removeLayerMap.put("panel", panelId);
		removeLayerMap.put("layer", layerId);
		
		// remove from the insight panel store
		InsightPanel panel = this.insight.getInsightPanel(panelId);
		if(panel == null) {
			throw new NullPointerException("Panel " + panelId + " does not exist");
		}
		panel.removeLayerViewOptions(layerId);
		
		return new NounMetadata(removeLayerMap, PixelDataType.REMOVE_LAYER, PixelOperationType.REMOVE_LAYER);
	}
}
