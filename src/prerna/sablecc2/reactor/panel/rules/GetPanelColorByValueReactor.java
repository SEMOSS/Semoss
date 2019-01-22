package prerna.sablecc2.reactor.panel.rules;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.om.ColorByValueRule;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPanelColorByValueReactor extends AbstractPanelColorByValueReactor {

	public GetPanelColorByValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		if(insightPanel == null) {
			throw new NullPointerException("Could not find insight panel");
		}
		List<ColorByValueRule> cbvRules = insightPanel.getColorByValue();
		List<Map<String, Object>> retList = new Vector<Map<String, Object>>();
		for(ColorByValueRule rule : cbvRules) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("panelId", insightPanel.getPanelId());
			retMap.put("name", rule.getId());
			retMap.put("filterInfo", rule.getQueryStruct().getExplicitFilters().getFormatedFilters());
			retMap.put("havingInfo", rule.getQueryStruct().getHavingFilters().getFormatedFilters());
			retMap.put("options", rule.getOptions());
			retList.add(retMap);
		}
		
		return new NounMetadata(retList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_COLOR_BY_VALUE_LIST);
	}
	
}
