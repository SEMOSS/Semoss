package prerna.reactor.panel.ornaments;

import prerna.om.InsightPanel;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemovePanelOrnamentsReactor extends AbstractInsightPanelReactor {
	
	public RemovePanelOrnamentsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.TRAVERSAL.getKey()};
	}

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
