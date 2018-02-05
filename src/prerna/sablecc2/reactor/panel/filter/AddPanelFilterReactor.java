package prerna.sablecc2.reactor.panel.filter;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.filter.AbstractFilterReactor;

public class AddPanelFilterReactor extends AbstractFilterReactor {
	
	public AddPanelFilterReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.FILTERS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		// get the filters
		GenRowFilters grf = getFilters();
		if(grf.isEmpty()) {
			throw new IllegalArgumentException("No filter found to add to panel");
		}
		panel.getPanelFilters().merge(grf);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.PANEL_FILTER);
		return noun;
	}

}
