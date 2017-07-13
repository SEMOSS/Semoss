package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class ClosePanelReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel panelToDelete = this.insight.getInsightPanels().remove(panelId);
		boolean success = true;
		if(panelToDelete == null) {
			success = false;
		}
		return new NounMetadata(success, PkslDataTypes.BOOLEAN);
	}
}
