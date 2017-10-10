package prerna.sablecc2.reactor.panel.ornaments;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;

public class ResetPanelOrnamentsReactor extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		insightPanel.resetOrnaments();
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_ORNAMENT);
	}

}
