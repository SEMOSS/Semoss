package prerna.sablecc2.reactor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class CollectReactor extends AbstractReactor{

	@Override
	public void In() {
		curNoun("all");
		printReactorTrace();
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	public Object execute() {
		Iterator<IHeadersDataRow> job = getJob();
		int collectThisMany = getTotalToCollect();
		
		List<Object> values = new ArrayList<>(collectThisMany);
		int i = 0;
		
		while(i < collectThisMany && job.hasNext()) {
			IHeadersDataRow nextData = job.next();
			values.add(nextData);
			i++;
		}
		
		NounMetadata result = new NounMetadata(values, PkslDataTypes.RAW_DATA_SET);
		return result;
	}
	
	@Override
	public void mergeUp() {
		
	}

	@Override
	public void updatePlan() {
		
	}
	
	private Iterator<IHeadersDataRow> getJob() {
		Iterator<IHeadersDataRow> job = (Iterator<IHeadersDataRow>)getNounStore().getNoun("JOB").get(0);
		return job;//(Iterator<IHeadersDataRow>)jobNoun.getValue();
	}
	
	private int getTotalToCollect() {
//		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		Double collectThisMany = (Double)curRow.get(0);
		return collectThisMany.intValue();
	}
}
