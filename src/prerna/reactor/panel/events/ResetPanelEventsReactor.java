package prerna.reactor.panel.events;

import prerna.om.InsightPanel;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ResetPanelEventsReactor extends AbstractInsightPanelReactor {
	
	public ResetPanelEventsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		insightPanel.resetEvents();
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_EVENT);
	}

}
