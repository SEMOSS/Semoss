package prerna.reactor;

import prerna.auth.User;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobThread;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;

public class StopPixelExecutionReactor extends AbstractReactor {
			
	public StopPixelExecutionReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ID.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		
		this.organizeKeys();
		
		String jobId = this.keyValue.get(ReactorKeysEnum.ID.getKey());
		
		PixelJobManager jobManager = PixelJobManager.getManager();
		
		jobManager.interruptThread(jobId);
		jobManager.clearJob(jobId);
		PixelJobThread pjt = jobManager.removeJob(jobId);
		
		SocketClient pySocketClient = user.getPythonSocketClient(false);
		if(pySocketClient != null) {
			pySocketClient.interruptInsight(pjt.getInsight().getInsightId());
		}
		
		return new NounMetadata("Pixel operation ended", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
	
	@Override
	public String getReactorDescription() {
		return "Stop the current execution of a pixel job";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equalsIgnoreCase(ReactorKeysEnum.ID.getKey()) ) {
			return "The id for the job. If running the pixel synchronously, the job id will be the same as the insight id.";
		}
		return super.getDescriptionForKey(key);
	}
}
