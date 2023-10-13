package prerna.reactor.panel;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PanelExistsReactor extends AbstractReactor {
	
	public PanelExistsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		// determine if the id currently exists
		boolean panelExists = this.insight.getInsightPanels().containsKey(panelId);
		return new NounMetadata(panelExists, PixelDataType.BOOLEAN);
	}
}
