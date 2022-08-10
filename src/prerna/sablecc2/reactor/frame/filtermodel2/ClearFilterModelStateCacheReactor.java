package prerna.sablecc2.reactor.frame.filtermodel2;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;
import prerna.util.Constants;

public class ClearFilterModelStateCacheReactor extends AbstractFilterReactor {

	private static final Logger logger = LogManager.getLogger(ClearFilterModelStateCacheReactor.class);
	
	public ClearFilterModelStateCacheReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		String panelId = this.curRow.get(0).toString();
		InsightPanel panelToClear = this.insight.getInsightPanels().get(panelId);
		if(panelToClear == null) {
			throw new IllegalArgumentException("Could not find panelId = " + panelId + " to clear.");
		}
		Map<String, ITableDataFrame> filterCaches = panelToClear.getCachedFilterModelFrame();
		for(String key : filterCaches.keySet()) {
			try {
				filterCaches.remove(key).close();
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
