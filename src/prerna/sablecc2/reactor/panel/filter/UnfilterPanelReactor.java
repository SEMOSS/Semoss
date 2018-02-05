package prerna.sablecc2.reactor.panel.filter;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;

public class UnfilterPanelReactor extends AbstractFilterReactor {

	public UnfilterPanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COLUMNS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		List<Object> colsToUnfilter = null;
		if(this.curRow.size() > 0) {
			colsToUnfilter = this.curRow.getValuesOfType(PixelDataType.CONST_STRING);
		}
		
		if(colsToUnfilter == null || colsToUnfilter.isEmpty()) {
			panel.getPanelFilters().removeAllFilters();
		} else {
			GenRowFilters grf = panel.getPanelFilters();
			for(Object col : colsToUnfilter) {
				grf.removeColumnFilter(col + "");
			}
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.PANEL_FILTER);
		return noun;
	}
}
