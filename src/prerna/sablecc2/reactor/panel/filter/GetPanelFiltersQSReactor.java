package prerna.sablecc2.reactor.panel.filter;

import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;

public class GetPanelFiltersQSReactor extends AbstractFilterReactor {

	public GetPanelFiltersQSReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PANEL.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		GenRowFilters filters = null;
		if (panel != null) {
			filters = panel.getPanelFilters();
		} else {
			throw new IllegalArgumentException("No frame currently exists within the insight");
		}
		
		SelectQueryStruct tempQs = new SelectQueryStruct();
		tempQs.setExplicitFilters(filters);
		return new NounMetadata(tempQs, PixelDataType.QUERY_STRUCT, PixelOperationType.PANEL_FILTER);
	}

}