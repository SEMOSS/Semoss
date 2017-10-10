package prerna.sablecc2.reactor.panel.events;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;

public class ResetPanelEventsReactor extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		insightPanel.resetEvents();
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_EVENT);
	}

}
