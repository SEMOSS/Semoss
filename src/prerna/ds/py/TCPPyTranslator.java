package prerna.ds.py;

import java.util.Hashtable;

import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.Client;

public class TCPPyTranslator extends PyTranslator {

	public static final String METHOD_DELIMITER = "$$##";
	public Client nc = null;
	String method = null;
	
	@Override
	public synchronized Object runScript(String script) 
	{
		if(method != null)
		{
			script = method +  METHOD_DELIMITER + script;
			method = null;
		}
		
		//System.out.println(".");
//		Object response = nc.executeCommand(script);
		//Object [] outputObj = (Object [])response;
		
		//System.out.println("Command was " + outputObj[0] + "<>" + script + "<>" + outputObj[1]);
		//System.err.println("Got the response back !!!!! WOO HOO " + response);
		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();

		PayloadStruct ps = constructPayload(methodName, script);
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.payloadClasses = new Class[] {String.class};
		ps.longRunning = true;
		if(nc.isConnected())
		{
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(ps != null && ps.ex != null)
			{
				logger.info("Exception " + ps.ex);
			}
			else
				return ps.payload[0];
		}
		else
		{
			logger.info("Py engine is not available anymore ");
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
		return null;
	}

	@Override
	protected synchronized Hashtable executePyDirect(String...script)
	{
		Hashtable retHash = new Hashtable();
		retHash.put(script, runScript(script[0])); 
		return retHash;
	}

	@Override
	protected void executeEmptyPyDirect(String script)
	{
		
		runScript(script);
	}

	private PayloadStruct constructPayload(String methodName, Object...objects )
	{
		// go through the objects and if they are set to null then make them as string null
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.R;
		ps.methodName = methodName;
		ps.payload = objects;
		
		return ps;
	}

	
	
}
