package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class ClosePanelReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel panelToDelete = this.insight.getInsightPanels().remove(panelId);
		if(panelToDelete == null) {
			throw new IllegalArgumentException("Could not find panelId = " + panelId + " to close.");
		}
		return new NounMetadata(panelId, PixelDataType.CONST_STRING, PixelOperationType.PANEL_CLOSE);
	}
}
