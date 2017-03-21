package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;

public class ExportReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	public Object execute() {
		return exportData();
	}
	
	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}

	private NounMetadata exportData() {
		
		Map<String, Object> exportedData = new HashMap<>();
		
		GenRowStruct chartNouns = getNounStore().getNoun("chart");
		Map<String, Object> widgetMap = new HashMap<>();
		for(Object noun : chartNouns) {
			widgetMap.put(noun.toString(), "data");
		}
		
		//assume 1 data for now
		GenRowStruct dataNouns = getNounStore().getNoun("data");
		for(Object noun : dataNouns) {
			exportedData.put("formattedData", ((NounMetadata)noun).getValue());
		}
		
		this.planner.addProperty("DATA", "DATA", exportedData);
		NounMetadata result = new NounMetadata(exportedData, "EXPORT");
		return result;
//		this.planner.addVariable("$RESULT", result);
	}
}
