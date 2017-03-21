package prerna.sablecc2.reactor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;

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
		
		NounMetadata result = new NounMetadata(values, "DATA");
		return result;
	}
	
	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}
	
	private Iterator<IHeadersDataRow> getJob() {
		NounMetadata jobNoun = (NounMetadata)getNounStore().getNoun("JOB").get(0);
		return (Iterator<IHeadersDataRow>)jobNoun.getValue();
	}
	
	private int getTotalToCollect() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		Double collectThisMany = (Double)allNouns.get(0);
		return collectThisMany.intValue();
	}
}
