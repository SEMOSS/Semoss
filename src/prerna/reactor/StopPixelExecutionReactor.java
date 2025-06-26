package prerna.reactor;


import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;

public class StopPixelExecutionReactor extends AbstractReactor {
		
	public StopPixelExecutionReactor() {}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		SocketClient socketClient = this.insight.getUser().getSocketClient(false);
		
		String killActiveThreads = socketClient.killActiveThreadsWithInterrupt();
		
		return new NounMetadata(killActiveThreads, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
