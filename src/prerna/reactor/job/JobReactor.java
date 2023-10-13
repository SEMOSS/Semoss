package prerna.reactor.job;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class JobReactor extends AbstractReactor {
	
	public static final String JOB_KEY = "$JOB_ID";
	public static final String SESSION_KEY = "$SESSION_ID";
	public static final String INSIGHT_KEY = "$INSIGHT_ID";
	
	private static final String CLASS_NAME = JobReactor.class.getName();
	
	public JobReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.JOB_ID.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.SESSION_ID.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// this just returns the job id
		if(curRow.size() > 0) {
			String jobId = this.curRow.get(0).toString();
			logger.debug("Job ID = " + jobId);
			planner.addVariable(JOB_KEY, new NounMetadata(jobId, PixelDataType.CONST_STRING));
		}
		
		if(curRow.size() > 1) {
			String insightId = this.curRow.get(1).toString();
			logger.debug("Insight ID = " + insightId);
			planner.addVariable(INSIGHT_KEY, new NounMetadata(insightId, PixelDataType.CONST_STRING));
		}
		
		if(curRow.size() > 2) {
			String sessionId = this.curRow.get(2).toString();
			logger.debug("Session ID = " + sessionId);
			planner.addVariable(SESSION_KEY, new NounMetadata(sessionId, PixelDataType.CONST_STRING));
		}

		return new NounMetadata(jobId, PixelDataType.CONST_STRING, PixelOperationType.JOB_ID);
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) {
			return outputs;
		}
		
		outputs = new Vector<NounMetadata>();
		// since output is lazy
		// just return the execute
		outputs.add( (NounMetadata) execute());
		return outputs;
	}
}
