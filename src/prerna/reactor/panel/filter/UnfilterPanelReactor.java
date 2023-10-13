package prerna.reactor.panel.filter;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class UnfilterPanelReactor extends AbstractFilterReactor {

	public UnfilterPanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COLUMNS.getKey(), TASK_REFRESH_KEY};
	}
	
	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		List<Object> colsToUnfilter = null;
		if(this.curRow.size() > 0) {
			colsToUnfilter = this.curRow.getValuesOfType(PixelDataType.CONST_STRING);
		}
		
		boolean hasFilters = false;
		if(colsToUnfilter == null || colsToUnfilter.isEmpty()) {
			GenRowFilters grf = panel.getPanelFilters();
			if(!grf.isEmpty()) {
				grf.removeAllFilters();
				hasFilters = true;
			}
		} else {
			GenRowFilters grf = panel.getPanelFilters();
			for(Object col : colsToUnfilter) {
				String colName = col + "";
				if(grf.hasFilter(colName)) {
					grf.removeColumnFilter(colName);
					hasFilters = true;
				}
			}
		}
		
		BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
		pFilterVal.setName(panel.getPanelId());
		pFilterVal.setFilterVal(hasFilters);
		NounMetadata noun = new NounMetadata(pFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_FILTER_CHANGE);
		if(hasFilters && isRefreshTasks()) {
			Logger logger = getLogger(UnfilterPanelReactor.class.getName());
			InsightUtility.addInsightPanelRefreshFromPanelFilter(insight, panel, noun, logger);
		}
		return noun;
	}
}
