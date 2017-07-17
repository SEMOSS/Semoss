package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddPanelReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel newPanel = new InsightPanel(panelId);
		this.insight.addNewInsightPanel(newPanel);
		NounMetadata noun = new NounMetadata(newPanel, PkslDataTypes.PANEL, PkslOperationTypes.PANEL_OPEN);
		return noun;
	}

}
