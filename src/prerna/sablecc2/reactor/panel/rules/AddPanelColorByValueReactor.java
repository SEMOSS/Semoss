package prerna.sablecc2.reactor.panel.rules;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddPanelColorByValueReactor extends AbstractPanelColorByValueReactor {
	
	public AddPanelColorByValueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), 
				ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.QUERY_STRUCT.getKey(),
				ReactorKeysEnum.OPTIONS.getKey()};
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
			throw new NullPointerException("Must define a name for this color by value rule");
		}
		SelectQueryStruct qs = getQs();
		if(qs == null) {
			throw new NullPointerException("Must define a query struct that is the color by value rule");
		}
		Map<String, Object> cbvOptions = getOptions();
//		if(cbvOptions == null) {
//			throw new NullPointerException("Must define the options associated with the color by value rule");
//		}

		// store the cbv
		insightPanel.getColorByValue().put(cbvRule, qs);

		// need to return
		// panelId
		// cbvRuleId (name)
		// filter info of the qs
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("panelId", insightPanel.getPanelId());
		retMap.put("name", cbvRule);
		retMap.put("filterInfo", qs.getExplicitFilters().getFormatedFilters());
		retMap.put("havingInfo", qs.getHavingFilters().getFormatedFilters());
		retMap.put("options", cbvOptions);
		return new NounMetadata(retMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ADD_PANEL_COLOR_BY_VALUE);
	}
	
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Get the QS for the rule
	 * @return
	 */
	private SelectQueryStruct getQs() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[2]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (SelectQueryStruct) genericReactorGrs.get(0);
		}
		
		genericReactorGrs = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (SelectQueryStruct) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return (SelectQueryStruct) strNouns.get(0).getValue();
		}
				
		// well, you are out of luck
		throw new NullPointerException("Must define a query struct that is the color by value rule");
	}
	
	/**
	 * Get the options for the color by value rule
	 * @return
	 */
	private Map<String, Object> getOptions() {
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[3]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, Object>) genericReactorGrs.get(0);
		}
		
		genericReactorGrs = this.store.getNoun(PixelDataType.MAP.toString());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, Object>) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return (Map<String, Object>) strNouns.get(0).getValue();
		}
		
		return null;
	}
}