package prerna.sablecc2.reactor.panel;

import java.util.HashMap;
import java.util.Map;

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
		Map<String, Object> ornamentData = new HashMap<String, Object>();
		// need to add the panel id so the FE knows which panel this is for
		ornamentData.put("panelId", insightPanel.getPanelId());
		if(traversal == null) {
			ornamentData.put("ornaments", insightPanel.getOrnaments());
		} else {
			ornamentData.put("ornaments", insightPanel.getOrnaments());
		}
		return new NounMetadata(ornamentData, PkslDataTypes.CUSTOM_DATA_STRUCTURE, PkslOperationTypes.PANEL_ORNAMENT);
	}

}
