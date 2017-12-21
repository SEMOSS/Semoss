package prerna.sablecc2.reactor.panel.sort;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class AddPanelSortReactor extends AbstractPanelSortReactor {

	public AddPanelSortReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.SORT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		// get the sort information
		List<QueryColumnOrderBySelector> sorts = getSortColumns();
		NounMetadata noun = null;
		if(sorts.isEmpty()) {
			noun = new NounMetadata("No Sort Information Found To Add", PixelDataType.CONST_STRING, PixelOperationType.WARNING);
		} else {
			panel.getPanelOrderBys().addAll(sorts);
			noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.PANEL_SORT);
		}
		
		return noun;
	}
	
}
