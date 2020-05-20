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
		InsightPanel panel = this.insight.getInsightPanels().get(panelId);
		if(panel == null) {
			String sheetId = this.keyValue.get(this.keysToGet[1]);
			if(sheetId == null) {
				sheetId = Insight.DEFAULT_SHEET_ID;
			}
			panel = new InsightPanel(panelId, sheetId);
			this.insight.addNewInsightPanel(panel);
			NounMetadata noun = new NounMetadata(panel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
			return noun;
		}
		NounMetadata noun = new NounMetadata(panel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN_IF_ABSENT);
		return noun;
	}
}
