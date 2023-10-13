package prerna.reactor.panel;

import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClosePanelIfExistsReactor extends AbstractReactor {
	
	public ClosePanelIfExistsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel panelToDelete = this.insight.getInsightPanels().remove(panelId);
		// just return saying we couldn't close the panel since it doesn't exist
		if(panelToDelete == null) {
			return new NounMetadata("Panel '" + panelId + "' does not exist", PixelDataType.CONST_STRING);
		}
		return new NounMetadata(panelId, PixelDataType.CONST_STRING, PixelOperationType.PANEL_CLOSE);
	}
}
