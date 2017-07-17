package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class PanelReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel insightPanel = this.insight.getInsightPanel(panelId);
		if(insightPanel == null) {
			throw new NullPointerException("Panel Id " + panelId + " does not exist");
		}
		NounMetadata noun = new NounMetadata(insightPanel, PkslDataTypes.PANEL, PkslOperationTypes.PANEL);
		return noun;
	}
}
