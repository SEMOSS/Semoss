package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;

public class ResetPanelOrnamentsReactor extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		insightPanel.resetOrnaments();
		return new NounMetadata(insightPanel, PkslDataTypes.PANEL, PkslOperationTypes.PANEL_ORNAMENT);
	}

}
