package prerna.reactor.panel;

import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClosePanelReactor extends AbstractReactor {
	
	public ClosePanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

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
