package prerna.sablecc2.reactor.export;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.om.PkslOperationTypes;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * 
 * This class is responsible for collecting data from a job and returning it
 *
 */
public class CollectReactor extends AbstractReactor {

	private static final String INCLUDE_META_KEY = "includeMeta";
	private static final String NUM_COLLECT_KEY = "limit";

	public NounMetadata execute() {
		Job job = getJob();
		int collectThisMany = getTotalToCollect();
		boolean collectMeta = collectMeta();
		Object data = job.collect(collectThisMany, collectMeta);
		NounMetadata result = new NounMetadata(data, PkslDataTypes.FORMATTED_DATA_SET, PkslOperationTypes.JOB_DATA);
		return result;
	}
	
	//This gets the Job collect reactor needs to collect from
	private Job getJob() {
		Job job;
		
		List<Object> jobs = curRow.getColumnsOfType(PkslDataTypes.JOB);
		//if we don't have jobs in the curRow, check if it exists in genrow under the key job
		if(jobs == null || jobs.size() == 0) {
			job = (Job) getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		} else {
			job = (Job) curRow.getColumnsOfType(PkslDataTypes.JOB).get(0);
		}
		return job;
	}
	
	//returns how much do we need to collect
	private int getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(NUM_COLLECT_KEY);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}
		
		// try the cur row
		List<Object> allNumericInputs = this.curRow.getAllNumericColumns();
		if(allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return ((Number) allNumericInputs.get(0)).intValue();
		}
		
		// default to 500
		return 500;
	}
	
	private boolean collectMeta() {
		// try the key
		GenRowStruct includeMetaGrs = store.getNoun(INCLUDE_META_KEY);
		if(includeMetaGrs != null && !includeMetaGrs.isEmpty()) {
			return (boolean) includeMetaGrs.get(0);
		}
		
		// try the cur row
		List<NounMetadata> booleanNouns = this.curRow.getNounsOfType(PkslDataTypes.BOOLEAN);
		if(booleanNouns != null && !booleanNouns.isEmpty()) {
			return (boolean) booleanNouns.get(0).getValue();
		}
		
		return true;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.FORMATTED_DATA_SET, PkslOperationTypes.JOB_DATA);
		outputs.add(output);
		return outputs;
	}
}
