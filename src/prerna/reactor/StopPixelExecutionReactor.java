package prerna.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.comm.PixelJobManager;

public class StopPixelExecutionReactor extends AbstractReactor {
			
	public StopPixelExecutionReactor() {
		// THIS IS A JOB ID
		this.keysToGet = new String[] {ReactorKeysEnum.ID.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String jobId = this.keyValue.get(ReactorKeysEnum.ID.getKey());
		
		PixelJobManager jobManager = PixelJobManager.getManager();
		
		jobManager.interruptThread(jobId);
		jobManager.clearJob(jobId);
		jobManager.removeJob(jobId);
		
		return new NounMetadata("Pixel operation ended", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
