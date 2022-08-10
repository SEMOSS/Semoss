package prerna.sablecc2.reactor.panel;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class ClosePanelReactor extends AbstractReactor {
	
	private static final Logger logger = LogManager.getLogger(ClosePanelReactor.class);
	
	public ClosePanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel panelToDelete = this.insight.getInsightPanels().remove(panelId);
		if(panelToDelete == null) {
			throw new IllegalArgumentException("Could not find panelId = " + panelId + " to close.");
		}
		Map<String, ITableDataFrame> filterCaches = panelToDelete.getCachedFilterModelFrame();
		for(String key : filterCaches.keySet()) {
			try {
				filterCaches.get(key).close();
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		return new NounMetadata(panelId, PixelDataType.CONST_STRING, PixelOperationType.PANEL_CLOSE);
	}
}
