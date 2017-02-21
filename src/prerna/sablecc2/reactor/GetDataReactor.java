package prerna.sablecc2.reactor;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;

import java.util.Iterator;
import java.util.Vector;


public class GetDataReactor extends AbstractReactor {


	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		getData();
		return parentReactor;
	}
	
	public void updatePlan() {

	}


	@Override
	protected void mergeUp() {
		//this reactor should not need to merge up
	}
	
	private void getData()  {
		//get the inputs
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		String jobId = (String)allNouns.get(0);
		Double limit = 0.0;
		if(allNouns.size() > 1) {
			limit = (Double)allNouns.get(1);
		}
		
		int count = 0;
		Iterator<IHeadersDataRow> iterator = JobStore.INSTANCE.getJob(jobId);
		while(limit > count && iterator.hasNext()) {
			
			System.out.println(iterator.next());
			count++;
		}
	}

	@Override
	public Vector<NounMetadata> getInputs() {
		// TODO Auto-generated method stub
		return null;
	}
}


