package prerna.reactor.frame.filtermodel2;

import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetFilterModelStateReactor extends AbstractFilterReactor {

	public SetFilterModelStateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.FILTERS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		if(panel == null) {
			throw new NullPointerException("Cannot find the input panel for set panel filter");
		}
		GenRowFilters panelTempModelGrf = panel.getTempFilterModelGrf();

		// get the filters to add
		GenRowFilters newFiltersToAdd = getFilters();
		if (newFiltersToAdd.isEmpty()) {
			throw new IllegalArgumentException("No filter found to set to panel");
		}

		// get the frame (or default frame)
		ITableDataFrame frame = getFrame();
		
		// check if we are filtering or actually removing a filter
		if(newFiltersToAdd.size() == 1) {
			IQueryFilter singleFilter = newFiltersToAdd.getFilters().get(0);
			if(singleFilter instanceof SimpleQueryFilter) {
				boolean unfilter = ((SimpleQueryFilter) singleFilter).isEmptyFilterValues();
				if(unfilter) {
					QueryColumnSelector cSelector = singleFilter.getAllQueryColumns().get(0);
					boolean isValidFilter = panelTempModelGrf.removeColumnFilter(cSelector.getAlias());

					panel.setTempFitlerModelFrame(frame);
					BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
					pFilterVal.setName(panel.getPanelId());
					pFilterVal.setFilterVal(isValidFilter);
					NounMetadata noun = new NounMetadata(pFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_FILTER_CHANGE);
					return noun;
				}
			}
		}
		
		// we got to this point, apply the filter
		// remove the existing filters for the columns affected
		Set<String> allColsUsed = newFiltersToAdd.getAllFilteredColumns();
		panelTempModelGrf.removeColumnFilters(allColsUsed);
		// add the new filters
		panelTempModelGrf.merge(newFiltersToAdd);
		
		panel.setTempFitlerModelFrame(frame);
		BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
		pFilterVal.setName(panel.getPanelId());
		pFilterVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(pFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_FILTER_CHANGE);
		return noun;
	}
	
}
