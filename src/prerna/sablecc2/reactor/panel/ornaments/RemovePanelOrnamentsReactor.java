package prerna.sablecc2.reactor.panel.ornaments;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;

public class RemovePanelOrnamentsReactor extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the traversal to the ornament to remove
		String traversal = getTraversalLiteralInput();
		if(traversal == null) {
			insightPanel.resetOrnaments();
		} else {
			insightPanel.removeOrnament(traversal);
		}
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_ORNAMENT);
	}

}
