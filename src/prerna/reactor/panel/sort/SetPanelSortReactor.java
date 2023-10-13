package prerna.reactor.panel.sort;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetPanelSortReactor extends AbstractPanelSortReactor {

	public SetPanelSortReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.SORT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		
		// get the sort information
		List<IQuerySort> sorts = getColumnSortBys();
		// set it in the panel
		panel.setPanelOrderBys(sorts);
		
		BooleanValMetadata pSortVal = BooleanValMetadata.getPanelVal();
		pSortVal.setName(panel.getPanelId());
		pSortVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(pSortVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_SORT);
		return noun;
	}
	
}
