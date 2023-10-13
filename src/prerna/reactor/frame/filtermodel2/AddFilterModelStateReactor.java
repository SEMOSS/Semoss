package prerna.reactor.frame.filtermodel2;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddFilterModelStateReactor extends AbstractFilterReactor {

	public AddFilterModelStateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.FILTERS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		if(panel == null) {
			throw new NullPointerException("Cannot find the input panel for add panel filter");
		}
		
		// get the filters to add
		GenRowFilters newFiltersToAdd = getFilters();
		if (newFiltersToAdd.isEmpty()) {
			throw new IllegalArgumentException("No filter found to add to panel");
		}
		
		// get the frame (or default frame)
		ITableDataFrame frame = getFrame();

		// get existing filters
		GenRowFilters existingFilters = panel.getTempFilterModelGrf();
		// add the new filters by merging into the existing state
		mergeFilters(newFiltersToAdd, existingFilters);
		
		panel.setTempFitlerModelFrame(frame);
		BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
		pFilterVal.setName(panel.getPanelId());
		pFilterVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(pFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_FILTER_CHANGE);
		return noun;
	}
	
}
