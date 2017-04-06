package prerna.sablecc2.reactor.export.job;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class JobBuilderReactor extends AbstractReactor {

	Job job;

	//This method is implemented by child classes, each class is responsible for building different pieces of the job
	protected abstract void buildJob();
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	public NounMetadata execute() {
		init(); //initialize the job
		buildJob(); //build the job
		return new NounMetadata(job, PkslDataTypes.JOB); //return the data
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		// all of the classes return the same thing
		// which is a job
		// this works because even if execute hasn't occured yet
		// because the same preference exists for the job
		// and since out is called prior to update the planner
		// the  cannot be null
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(job, PkslDataTypes.JOB);
		outputs.add(output);
		return outputs;
	}
	
	//initialize the reactor with its necessary inputs
	//We want the abstract to grab the job so the children only are responsible for building on this job
	private void init() {
		GenRowStruct jobInputParams = getNounStore().getNoun(PkslDataTypes.JOB.toString());
		if(jobInputParams != null) {
			this.job = (Job)jobInputParams.get(0);
		}
	}
}
