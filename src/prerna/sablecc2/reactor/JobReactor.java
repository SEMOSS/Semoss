package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class JobReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	public Object execute() {
		// this just returns the job id
		String jobId = (String)curRow.get(0);
		return new NounMetadata(jobId, PkslDataTypes.JOB);
	}

	@Override
	public void mergeUp() {
		
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		// since output is lazy
		// just return the execute
		outputs.add( (NounMetadata) execute());
		return outputs;
	}

	@Override
	public List<NounMetadata> getInputs() {
		// return the job id
		List<NounMetadata> inputs = new Vector<NounMetadata>();
		inputs.add(curRow.getNoun(0));
		return inputs;
	}


}
