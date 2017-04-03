package prerna.sablecc2.reactor;

import java.util.Iterator;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.NounMetadata;

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
	public void mergeUp() {
		//this reactor should not need to merge up
	}
	
	private void getData()  {
		//get the inputs
//		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //should be only joins
		String jobId = (String)curRow.get(0);
		Double limit = 0.0;
		if(curRow.size() > 1) {
			limit = (Double)curRow.get(1);
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


