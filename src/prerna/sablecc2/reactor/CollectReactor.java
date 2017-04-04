package prerna.sablecc2.reactor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.JobStore;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class CollectReactor extends AbstractReactor{

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	public Object execute() {
		// get the iterator corresponding to this job
		Iterator<IHeadersDataRow> iterator = getIterator(this.curRow.getColumnsOfType(PkslDataTypes.JOB).get(0).toString());
		// get the number of rows to return
		// if this is not pushed into the QS
		int collectThisMany = getTotalToCollect();
		
		List<Object> values = new ArrayList<>(collectThisMany);
		int i = 0;
		
		while(i < collectThisMany && iterator.hasNext()) {
			IHeadersDataRow nextData = iterator.next();
			values.add(nextData);
			i++;
		}
		
		NounMetadata result = new NounMetadata(values, PkslDataTypes.RAW_DATA_SET);
		return result;
	}
	
	private Iterator<IHeadersDataRow> getIterator(String jobId) {
		Iterator<IHeadersDataRow> iterator = JobStore.INSTANCE.getJob(jobId);
		if(iterator != null) return iterator;
		
		Object varObj = this.planner.getVariable(jobId);
		if(varObj != null && varObj instanceof Iterator) {
			return (Iterator<IHeadersDataRow>) varObj;
		}
		
		else throw new IllegalArgumentException("Argument not an Iterator");
	}
	
	private int getTotalToCollect() {
		Number collectThisMany = (Number) curRow.getColumnsOfType(PkslDataTypes.CONST_DECIMAL).get(0);
		return collectThisMany.intValue();
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.RAW_DATA_SET);
		outputs.add(output);
		return outputs;
	}
}
