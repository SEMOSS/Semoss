package prerna.sablecc2.reactor.panel.rules;

import java.util.HashMap;
import java.util.Map;

import prerna.om.ColorByValueRule;
import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
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
		if(insightPanel == null) {
			throw new NullPointerException("Could not find insight panel");
		}
		String cbvRule = getCbvId(1);
		if(cbvRule == null) {
			throw new NullPointerException("Must provide the color by value name within the panel");
		}
		ColorByValueRule rule = insightPanel.getColorByValue(cbvRule);
		if(rule == null) {
			throw new NullPointerException("Could not find the color by value rule within the panel");
		}
		SelectQueryStruct qs = rule.getQueryStruct();
		
		// make a copy
		// so that the original QS is not modified if additional changes
		// are added to the rule
		SelectQueryStruct newQs = new SelectQueryStruct();
		newQs.merge(qs);
		newQs.setDistinct(qs.isDistinct());
		newQs.setOverrideImplicit(qs.isOverrideImplicit());
		
		NounMetadata retNoun = new NounMetadata(newQs, PixelDataType.QUERY_STRUCT);
		// we need to append some additional metadata to this noun
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("panelId", insightPanel.getPanelId());
		retMap.put("name", cbvRule);
		retMap.put("type", "color");
		NounMetadata additionalNoun = new NounMetadata(retMap, PixelDataType.ORNAMENT_MAP, PixelOperationType.PANEL_COLOR_BY_VALUE);
		retNoun.addAdditionalReturn(additionalNoun);
		return retNoun;
	}
}