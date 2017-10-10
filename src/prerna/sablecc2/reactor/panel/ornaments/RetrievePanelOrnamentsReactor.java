package prerna.sablecc2.reactor.panel.ornaments;

import java.util.HashMap;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.panel.AbstractInsightPanelReactor;

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
			ornamentData.put("path", traversal);
			ornamentData.put("ornaments", insightPanel.getMapInput(insightPanel.getOrnaments(), traversal));
		}
		return new NounMetadata(ornamentData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_ORNAMENT);
	}

}
