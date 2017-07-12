package prerna.sablecc2.reactor.panel;

import java.util.Map;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class AddPanelOrnamentsReactor extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the ornaments that come as a map
		Map<String, Object> ornaments = getOrnamentsMapInput();
		// merge the map options
		insightPanel.setOrnaments(ornaments);
		return new NounMetadata(insightPanel, PkslDataTypes.PANEL);
	}

}
