package prerna.reactor.panel.sort;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddPanelSortByReactor extends AbstractPanelSortReactor {

	public AddPanelSortByReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUES.getKey()};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();

		// get the sort information
		IQuerySort sortBy = getCustomSortBy();
		panel.getPanelOrderBys().add(sortBy);
		
		BooleanValMetadata pSortVal = BooleanValMetadata.getPanelVal();
		pSortVal.setName(panel.getPanelId());
		pSortVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(pSortVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_SORT);
		return noun;
	}

}
