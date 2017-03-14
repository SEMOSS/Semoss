package prerna.sablecc2.reactor;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class JobReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
		
	}

	@Override
	public Object Out() {
		setJob();
		return null;
	}

	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}
	
	private void setJob() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		String jobId = (String)allNouns.get(0);
		
		Iterator<IHeadersDataRow> iterator = getIterator(jobId);
		this.planner.addProperty("RESULT", "RESULT", iterator);
		this.propStore.put("JOB", iterator);
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
