package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;

public class PanelCloneReactor extends AbstractInsightPanelReactor {

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel existingPanel = getInsightPanel();

		// get the new panel id
		String newId = getClonePanelId();
		// make the new panel
		InsightPanel newClonePanel = new InsightPanel(newId);
		newClonePanel.clone(existingPanel);
		
		// remember to add the new panel into the insight
		this.insight.addNewInsightPanel(newClonePanel);
		
		// return the new panel
		return new NounMetadata(newClonePanel, PkslDataTypes.PANEL, PkslOperationTypes.PANEL_CLONE);
	}
	
}