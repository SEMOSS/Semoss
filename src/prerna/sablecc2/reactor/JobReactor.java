package prerna.sablecc2.reactor;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;

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
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		String jobId = (String)allNouns.get(0);
		
		Iterator<IHeadersDataRow> iterator = getIterator(jobId);
		
		return new NounMetadata(iterator, "JOB");
	}

	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}
	
	private Iterator getIterator(String jobId) {
		Iterator<IHeadersDataRow> iterator = JobStore.INSTANCE.getJob(jobId);
		if(iterator != null) return iterator;
		
		Object varObj = this.planner.getVariable(jobId);
		if(varObj != null && varObj instanceof Iterator) {
			return (Iterator)varObj;
		}
		
		else throw new IllegalArgumentException("Argument not an Iterator");
	}

}
