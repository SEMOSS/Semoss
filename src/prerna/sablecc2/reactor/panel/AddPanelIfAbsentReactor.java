package prerna.sablecc2.reactor.panel;

import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddPanelIfAbsentReactor extends AbstractReactor {
	
	public AddPanelIfAbsentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel newPanel = this.insight.getInsightPanels().get(panelId);
		if(newPanel == null) {
			String sheetId = this.keyValue.get(this.keysToGet[1]);
			if(sheetId == null) {
				sheetId = Insight.DEFAULT_SHEET;
			}
			newPanel = new InsightPanel(panelId, sheetId);
			this.insight.addNewInsightPanel(newPanel);
		}
		NounMetadata noun = new NounMetadata(newPanel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
		return noun;
	}
}
