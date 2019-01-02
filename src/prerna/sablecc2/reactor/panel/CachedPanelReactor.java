package prerna.sablecc2.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class CachedPanelReactor extends AbstractReactor {
	
	/**
	 * This code is the same as the Panel Reactor
	 * But it has a different op type
	 * 
	 * It is only intended to be used to simplify the 
	 * cached insight recipe into a single call to get the panel
	 * state instead of multiple calls for each portion of the insight
	 * 
	 */
	
	public CachedPanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel insightPanel = this.insight.getInsightPanel(panelId);
		if(insightPanel == null) {
			throw new NullPointerException("Panel Id " + panelId + " does not exist");
		}
		NounMetadata noun = new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.CACHED_PANEL);
		return noun;
	}
}
