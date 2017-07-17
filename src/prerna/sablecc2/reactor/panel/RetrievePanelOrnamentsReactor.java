package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;

public class RetrievePanelOrnamentsReactor extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		String traversal = getTraversalLiteralInput();
		Object ornamentData = null;
		if(traversal == null) {
			ornamentData = insightPanel.getOrnaments();
		} else {
			ornamentData = insightPanel.getOrnament(traversal);
		}
		return new NounMetadata(ornamentData, PkslDataTypes.CUSTOM_DATA_STRUCTURE, PkslOperationTypes.PANEL_ORNAMENT);
	}

}
