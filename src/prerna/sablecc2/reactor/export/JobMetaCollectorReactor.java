package prerna.sablecc2.reactor.export;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class JobMetaCollectorReactor extends AbstractReactor {

	private static final String VIEW_OPTIONS = "VIEWOPTIONS";
	private static final String HEADER_INFO = "HEADERINFO";
	private static final String VIEWS = "VIEWS";
	private static final String TARGETS = "TARGETS";

	public NounMetadata execute() {
		Job job = getJob();
		
		Map<String, Object> metaData = new HashMap<String, Object>(3);
		// get all the strings that were passed
		// and figure out which pieces of metadata to return
		List<NounMetadata> passedInStrings = this.curRow.getNounsOfType(PkslDataTypes.CONST_STRING);
		int size = passedInStrings.size();
		for(int i = 0; i < size; i++) {
			String valToRetrieve = passedInStrings.get(i).getValue().toString().trim().toUpperCase();
			if(VIEW_OPTIONS.equals(valToRetrieve)) {
				metaData.put("viewOptions", job.getViewOptions());
			} else if(HEADER_INFO.equals(valToRetrieve)) {
				metaData.put("headerInfo", job.getHeaderInfo());
			} else if(VIEWS.equals(valToRetrieve)) {
				metaData.put("view", job.getViews());
			} else if(TARGETS.equals(valToRetrieve)) {
				metaData.put("targets", job.getTargets());
			}
		}
		metaData.put("jobId", job.getId());
		
		NounMetadata result = new NounMetadata(metaData, PkslDataTypes.FORMATTED_DATA_SET);
		
		return result;
	}
	
	//This gets the Job collect reactor needs to collect from
	private Job getJob() {
		Job job;
		
		List<Object> jobs = curRow.getColumnsOfType(PkslDataTypes.JOB);
		if(jobs == null || jobs.size() == 0) {
			job = (Job) getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		} else {
			job = (Job) curRow.getColumnsOfType(PkslDataTypes.JOB).get(0);
		}
		return job;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.FORMATTED_DATA_SET);
		outputs.add(output);
		return outputs;
	}
}