package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

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
		Map<String, Object> exportedData = new HashMap<>();
		
		//assume 1 data for now
		GenRowStruct dataNouns = getNounStore().getNoun("data");
		int numNouns = dataNouns.size();
		for(int nounIdx = 0; nounIdx < numNouns; nounIdx++) {
			exportedData = (Map<String, Object>) (dataNouns.getNoun(nounIdx)).getValue();
		}
		this.planner.addProperty("DATA", "DATA", exportedData);
		NounMetadata result = new NounMetadata(exportedData, PkslDataTypes.EXPORT);
		return result;
	}
	
	@Override
	public void mergeUp() {
		
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.EXPORT);
		outputs.add(output);
		return outputs;
	}
	
	@Override
	public List<NounMetadata> getInputs() {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		return null;
	}
}
