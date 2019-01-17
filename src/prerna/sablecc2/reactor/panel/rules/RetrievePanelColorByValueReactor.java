package prerna.sablecc2.reactor.panel.rules;

import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RetrievePanelColorByValueReactor extends AbstractPanelColorByValueReactor {
	
	public RetrievePanelColorByValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.NAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		SelectQueryStruct qs = insightPanel.getColorByValue().get(getCbvId(1));
		// make a copy
		// so that the original QS is not modified if additional changes
		// are added to the rule
		SelectQueryStruct newQs = new SelectQueryStruct();
		newQs.merge(qs);
		newQs.setDistinct(qs.isDistinct());
		newQs.setOverrideImplicit(qs.isOverrideImplicit());
		return new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
	}
}