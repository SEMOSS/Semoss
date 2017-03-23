package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

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
		
		//assume 1 data for now
		GenRowStruct dataNouns = getNounStore().getNoun("data");
		int numNouns = dataNouns.size();
		for(int nounIdx = 0; nounIdx < numNouns; nounIdx++) {
			exportedData = (Map<String, Object>) ((NounMetadata)dataNouns.get(nounIdx)).getValue();
		}
		this.planner.addProperty("DATA", "DATA", exportedData);
		NounMetadata result = new NounMetadata(exportedData, PkslDataTypes.EXPORT);
		return result;
//		this.planner.addVariable("$RESULT", result);
	}
}
