package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddPanelIfAbsentReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel newPanel = this.insight.getInsightPanels().get(panelId);
		if(newPanel == null) {
			newPanel = new InsightPanel(panelId);
			this.insight.addNewInsightPanel(newPanel);
		}
		NounMetadata noun = new NounMetadata(newPanel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
		return noun;
	}
}
