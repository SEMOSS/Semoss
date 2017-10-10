package prerna.sablecc2.reactor.panel.sort;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.QueryColumnOrderBySelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class RemovePanelSortReactor extends AbstractPanelSortReactor {

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		// get the sort information
		List<QueryColumnOrderBySelector> sorts = getSortColumns();
		if(sorts.isEmpty()) {
			// we want to remove everything
			panel.getPanelOrderBys().clear();
		} else {
			// find the specific sorts to remove
			List<QueryColumnOrderBySelector> curPanelSorts = panel.getPanelOrderBys();
			int numExistingSorts = curPanelSorts.size();
			if(numExistingSorts != 0) {
				REMOVE_SORT_LOOP : for(QueryColumnOrderBySelector removeThisSort : sorts) {
					String removeThisQsName = removeThisSort.getQueryStructName();
					for(int i = 0; i < numExistingSorts; i++) {
						String existingQsName = curPanelSorts.get(i).getQueryStructName();
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
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.PANEL_SORT);
	}
}