package prerna.tcp.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.IReactor;
import prerna.tcp.PayloadStruct;

public class CustomReactorWrapper extends AbstractReactor {

	// this takes the custom reactor
	// sets up to go across socket
	// returns the result
	public IReactor realReactor = null;
	public String reactorCallName = null;
	SocketClient sc = null;
	private static final Logger logger = LogManager.getLogger(WrapperManager.class);

	
	
	@Override
	public NounMetadata execute() {
		
		sc = (SocketClient)this.insight.getUser().getTCPServer(true);

		
		pushInsight();
		
		// TODO Auto-generated method stub
		PayloadStruct ps = new PayloadStruct();
		ps.operation = ps.operation.REACTOR;
		
		// set everything from the noun store
		// hopefully this serializes well
		ps.payload = new Object [] {this.store};
		ps.payloadClasses = new Class[] {this.store.getClass()};
		ps.objId = reactorCallName;
		ps.insightId = this.insight.getInsightId();
		
		PayloadStruct retStruct = (PayloadStruct)sc.executeCommand(ps);
		
		logger.info("Got the response for reactor " + ps.payload[0]);
		
		return (NounMetadata)retStruct.payload[0];
	}
	
	private void pushInsight()
	{
		// see if the insight has been serialized
		// meh may be even synchronize on insight
		//synchronized(insight) - this will un-necessarily block. will deal when we get to it
		{
			if(!this.insight.serialized)
			{
				PayloadStruct ps = new PayloadStruct();
				ps.operation = ps.operation.INSIGHT;
				
				// set the sc
				
				// set everything from the noun store
				// hopefully this serializes well
				ps.payload = new Object [] {this.insight};
				ps.payloadClasses = new Class[] {this.insight.getClass()};
				ps.objId = realReactor.getName();
				ps.hasReturn = false;
				
				PayloadStruct retStruct = (PayloadStruct)sc.executeCommand(ps);
				
				this.insight.serialized = true;
			}
		}
	}

}
