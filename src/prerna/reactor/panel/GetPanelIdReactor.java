package prerna.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPanelIdReactor extends AbstractInsightPanelReactor {
	
	public GetPanelIdReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		return new NounMetadata(insightPanel.getPanelId(), PixelDataType.CONST_STRING);
	}
}