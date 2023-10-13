package prerna.reactor.panel.sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetMultiTypePanelSortReactor extends AbstractPanelSortReactor {

	private static final String MAP_SORT_LIST = "sortConfig";
	
	public SetMultiTypePanelSortReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), MAP_SORT_LIST};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		// get the sort information
		List<Map<String, Object>> sortConfig = getSortConfig();
		if(sortConfig == null || sortConfig.isEmpty()) {
			throw new IllegalArgumentException("Must define the sort config");
		}
		
		int numSorts = sortConfig.size();
		List<IQuerySort> sorts = new ArrayList<>(numSorts);
		for(int sortIdx = 0; sortIdx < numSorts; sortIdx++) {
			Map<String, Object> sortMap = sortConfig.get(sortIdx);
			
			// first get the type
			String type = sortMap.get("type").toString().toLowerCase();
			if(type.equals("column")) {
				// this has 2 inputs
				// columns and sorts 
				
				List<String> colInputs = (List<String>) sortMap.get(ReactorKeysEnum.COLUMNS.getKey());
				List<String> sortDirs = (List<String>) sortMap.get(ReactorKeysEnum.SORT.getKey());

				// you can set within this map 
				// multiple sorts in one go as well
				int colSize = colInputs.size();
				int sortDirSize = sortDirs.size();
				for(int selectIndex = 0; selectIndex < colSize; selectIndex++) {
					String newSelector = colInputs.get(selectIndex);
					QueryColumnOrderBySelector selector = new QueryColumnOrderBySelector();
					if(newSelector.contains("__")) {
						String[] selectorSplit = newSelector.split("__");
						selector.setTable(selectorSplit[0]);
						selector.setColumn(selectorSplit[1]);
					} else {
						selector.setTable(newSelector);
						selector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
					}
					if(sortDirSize > selectIndex) {
						selector.setSortDir(sortDirs.get(selectIndex));
					} else {
						selector.setSortDir("ASC");
					}
					
					// just add to the overall sorts in order
					sorts.add(selector);
				}
				
			} else if(type.equals("custom")) {
				// this has 2 inputs
				// column and values
				
				String columnToSort = (String) sortMap.get(ReactorKeysEnum.COLUMN.getKey());
				List<Object> valuesForSort = (List<Object>) sortMap.get(ReactorKeysEnum.VALUES.getKey());
				
				QueryCustomOrderBy customSort = new QueryCustomOrderBy();
				customSort.setColumnToSort(new QueryColumnSelector(columnToSort));
				customSort.setCustomOrder(valuesForSort);
				sorts.add(customSort);
			}
		}
		
		// set this new list into the panel order by
		panel.setPanelOrderBys(sorts);
		
		BooleanValMetadata pSortVal = BooleanValMetadata.getPanelVal();
		pSortVal.setName(panel.getPanelId());
		pSortVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(pSortVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_SORT);
		return noun;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(MAP_SORT_LIST)) {
			return "A list of maps containing individual sort details that can represent a column or custom sort";
		}
		return super.getDescriptionForKey(key);
	}
	
	private List<Map<String, Object>> getSortConfig() {
		// if it was passed based on the key
		List<Map<String, Object>> colInputs = new Vector<>();
		GenRowStruct colsGrs = this.store.getNoun(MAP_SORT_LIST);
		if(colsGrs != null) {
			int size = colsGrs.size();
			if(size > 0) {
				for(int i = 0; i < size; i++) {
					colInputs.add((Map<String, Object>) colsGrs.get(i));
				}
				return colInputs;
			}
		}
		
		// if it was passed directly in
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		int size = mapInputs.size();
		for(int i = 0; i < size; i++) {
			colInputs.add((Map<String, Object>) mapInputs.get(i).getValue());
		}
		return colInputs;
	}
	
}
