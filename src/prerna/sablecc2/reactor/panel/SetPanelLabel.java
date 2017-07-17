package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;

public class SetPanelLabel extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the ornaments that come as a map
		String panelLabel = getPanelLabel();
		// merge the map options
		insightPanel.setPanelLabel(panelLabel);
		return new NounMetadata(insightPanel, PkslDataTypes.PANEL, PkslOperationTypes.PANEL_ORNAMENT);
	}
	
}
