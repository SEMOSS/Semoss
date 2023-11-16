package prerna.tcp.client;

import prerna.om.Insight;
import prerna.tcp.PayloadStruct;

public class InsightSerializer 
{

	Insight insight = null;
	
	public InsightSerializer(Insight insight)
	{
		this.insight = insight;
	}
	
	public void serializeInsight(boolean force)
	{
		// see if the insight has been serialized
		// meh may be even synchronize on insight
		//synchronized(insight) - this will un-necessarily block. will deal when we get to it
		{
			SocketClient sc = (SocketClient)this.insight.getUser().getSocketClient(true);
			if(!this.insight.getSerialized() || this.insight.getContextReinitialized() || force)
			{
				PayloadStruct ps = new PayloadStruct();
				ps.operation = ps.operation.INSIGHT;
				
				// set everything from the noun store
				// hopefully this serializes well
				ps.payload = new Object [] {this.insight};
				ps.payloadClasses = new Class[] {this.insight.getClass()};
				ps.hasReturn = false;
				
				PayloadStruct retStruct = (PayloadStruct)sc.executeCommand(ps);
				
				this.insight.setSerialized(true);
				this.insight.setContextReinitialized(false);
			}
		}
	}


}
