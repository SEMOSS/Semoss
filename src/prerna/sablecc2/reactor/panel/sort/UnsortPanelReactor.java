package prerna.sablecc2.reactor.panel.sort;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UnsortPanelReactor extends AbstractPanelSortReactor {

	public UnsortPanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.SORT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		// get the sort information
		List<IQuerySort> sorts = getColumnSortBys();
		if(sorts.isEmpty()) {
			// we want to remove everything
			panel.getPanelOrderBys().clear();
		} else {
			// find the specific sorts to remove
			List<IQuerySort> curPanelSorts = panel.getPanelOrderBys();
			int numExistingSorts = curPanelSorts.size();
			if(numExistingSorts != 0) {
				REMOVE_SORT_LOOP : for(IQuerySort removeThisSort : sorts) {
					if(removeThisSort.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
						String removeThisQsName = ((QueryColumnOrderBySelector) removeThisSort).getQueryStructName();
						for(int i = 0; i < numExistingSorts; i++) {
							IQuerySort curPanelSort = curPanelSorts.get(i);
							if(curPanelSort.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
								String existingQsName = ((QueryColumnOrderBySelector) curPanelSort).getQueryStructName();
								if(existingQsName.equals(removeThisQsName)) {
									// we found the one to remove
									curPanelSorts.remove(i);
									// update the number of loops for the next loop
									numExistingSorts--;
									// continue
									continue REMOVE_SORT_LOOP;
								}
							}
						}
					}
				}
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.PANEL_SORT);
	}
}