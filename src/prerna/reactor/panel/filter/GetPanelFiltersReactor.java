package prerna.reactor.panel.filter;

import java.util.ArrayList;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPanelFiltersReactor extends AbstractFilterReactor {

	public GetPanelFiltersReactor() {
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
		if (filters == null) {
			// just return an empty list
			return new NounMetadata(new ArrayList<Object>(), PixelDataType.FILTER, PixelOperationType.PANEL_FILTER);
		}
		return new NounMetadata(filters.getFormatedFilters(), PixelDataType.FILTER, PixelOperationType.PANEL_FILTER);
	}

}