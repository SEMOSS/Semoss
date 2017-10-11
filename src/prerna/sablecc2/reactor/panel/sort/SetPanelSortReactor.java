package prerna.sablecc2.reactor.panel.sort;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class SetPanelSortReactor extends AbstractPanelSortReactor {

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		// get the sort information
		List<QueryColumnOrderBySelector> sorts = getSortColumns();
		// set it in the panel
		panel.setPanelOrderBys(sorts);
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.PANEL_SORT);
	}
	
}
