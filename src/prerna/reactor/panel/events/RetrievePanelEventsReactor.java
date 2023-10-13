package prerna.reactor.panel.events;

import java.util.HashMap;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RetrievePanelEventsReactor extends AbstractInsightPanelReactor {

	public RetrievePanelEventsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.TRAVERSAL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		String traversal = getTraversalLiteralInput();
		Map<String, Object> eventsData = new HashMap<String, Object>();
		// need to add the panel id so the FE knows which panel this is for
		eventsData.put("panelId", insightPanel.getPanelId());
		if(traversal == null) {
			eventsData.put("events", insightPanel.getEvents());
		} else {
			eventsData.put("path", traversal);
			Object eventMap = insightPanel.getMapInput(insightPanel.getEvents(), traversal);
			if(eventMap == null) {
				eventMap = new HashMap<>();
			}
			eventsData.put("events", eventMap);
		}
		return new NounMetadata(eventsData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_EVENT);
	}

}
