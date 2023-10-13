package prerna.reactor.panel.sort;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPanelSortReactor extends AbstractPanelSortReactor {

	public GetPanelSortReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		if(panel == null) {
			throw new NullPointerException("Panel must be defined to retrieve the sorts");
		}
		// pull and send the panel sorts
		List<IQuerySort> orderBys = panel.getPanelOrderBys();
		NounMetadata noun = new NounMetadata(orderBys, PixelDataType.CUSTOM_DATA_STRUCTURE);
		return noun;
	}
	
}