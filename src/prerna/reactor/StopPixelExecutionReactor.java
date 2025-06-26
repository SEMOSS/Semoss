package prerna.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;
import prerna.sablecc2.comm.PixelJobManager;

public class StopPixelExecutionReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(StopPixelExecutionReactor.class);
		
	public StopPixelExecutionReactor() {
		// THIS IS A JOB ID
		this.keysToGet = new String[] {ReactorKeysEnum.ID.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		SocketClient socketClient = this.insight.getUser().getSocketClient(false);
		
		String killActiveThreads = socketClient.killActiveThreadsWithInterrupt();
		
		String jobId = this.keyValue.get(ReactorKeysEnum.ID.getKey());
		
		PixelJobManager jobManager = PixelJobManager.getManager();
		
		jobManager.clearJob(jobId);
		jobManager.removeJob(jobId);
		
		return new NounMetadata(killActiveThreads, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
