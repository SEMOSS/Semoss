package prerna.sablecc2.reactor.panel;

import java.util.Random;
import java.util.Set;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddPanelReactor extends AbstractReactor {
	
	private static Random rand = new Random();
	
	public AddPanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = null;
		organizeKeys();
		if(this.keyValue.isEmpty()) {
			// need to make sure the panel doesn't already exist
			panelId = rand.nextInt(5000) + "";
			Set<String> existingPanelIds = this.insight.getInsightPanels().keySet();
			while(existingPanelIds.contains(panelId)) {
				panelId = rand.nextInt(5000) + "";
			}
		} else {
			panelId = this.keyValue.get(this.keysToGet[0]);
		}
		InsightPanel newPanel = new InsightPanel(panelId);
		this.insight.addNewInsightPanel(newPanel);
		NounMetadata noun = new NounMetadata(newPanel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
		return noun;
	}

}
