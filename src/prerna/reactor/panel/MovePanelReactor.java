package prerna.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MovePanelReactor extends AbstractInsightPanelReactor {
	
	public MovePanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get the insight panel
		InsightPanel existingPanel = getInsightPanel();
		String sheetId = this.keyValue.get(this.keysToGet[1]);
		if(sheetId == null) {
			throw new NullPointerException("Must define the sheet where the panel should be moved");
		}
		if(existingPanel.getSheetId().equals(sheetId)) {
			throw new IllegalArgumentException("The sheet passed is the same as the panels current sheet");
		}
		existingPanel.setSheetId(sheetId);
		return new NounMetadata(existingPanel, PixelDataType.PANEL, PixelOperationType.PANEL_MOVE);
	}
	
}