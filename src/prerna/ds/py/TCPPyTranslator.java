package prerna.ds.py;

import java.util.Hashtable;

import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.ErrorSenderThread;
import prerna.tcp.client.SocketClient;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class TCPPyTranslator extends PyTranslator {

	public static final String METHOD_DELIMITER = "$$##";
	private SocketClient sc = null;
	private String method = null;
	
	/**
	 * 
	 * @param sc
	 */
	public void setSocketClient(SocketClient sc) {
		this.sc = sc;
	}
	
	/**
	 * 
	 * @return
	 */
	public SocketClient getSocketClient() {
		return this.sc;
	}

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
		if(sc.isConnected())
		{
			ps = (PayloadStruct)sc.executeCommand(ps);
			if(ps != null && ps.ex != null)
			{
				logger.info("Exception " + ps.ex);
				throw new SemossPixelException(ps.ex);
			}
			else
				return ps.payload[0];
		}
		else
		{
			logger.info("Py engine is not available anymore ");
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	// use this if we want to get the output from an operation
	// typically useful for model type operations
	@Override
	public synchronized Object runScript(String script, Insight insight) 
	{
		if(method != null)
		{
			script = method +  METHOD_DELIMITER + script;
			method = null;
		}

		String methodName = new Object(){}.getClass().getEnclosingMethod().getName();

		PayloadStruct ps = constructPayload(methodName, script);
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.payloadClasses = new Class[] {String.class};
		ps.longRunning = true;
		
		ErrorSenderThread est = null;
		String file = null;
		
		// get error messages
		if(insight != null)
		{
			ps.insightId = insight.getInsightId();
			if(insight.getUser() != null)
			{
				sc.addInsight2Insight(ps.insightId, insight);
			}
			boolean nativePyServer = DIHelper.getInstance().getProperty(Settings.NATIVE_PY_SERVER) != null
					&& DIHelper.getInstance().getProperty(Settings.NATIVE_PY_SERVER).equalsIgnoreCase("true");

			if(!nativePyServer)
			{
				est = new ErrorSenderThread();
				est.setInsight(insight);
	
				// write the file to create
				// for now let it be
				file = Utility.getRandomString(5);
				makeTempFolder(insight.getInsightFolder());
				String pyTemp  = insight.getInsightFolder() + "/Py/Temp";
				file = pyTemp + "/" + file;
				file = file.replace("\\", "/");
				
				script = script.replace("\"", "'");
				String payload = "smssutil.runwrappereval_return(\"" + script + "\", '" + file + "', '" + file + "', globals())";
				ps.payload[0] = payload;
				est.setFile(file);
				est.start();
			}
			//}
		}
		
		
		if(sc.isConnected())
		{
			ps = (PayloadStruct)sc.executeCommand(ps);
			if(est != null)
				est.stopSession();
			if(ps != null && ps.ex != null)
			{
				logger.info("Exception " + ps.ex);
				throw new SemossPixelException(ps.ex);
			}
			else
				return ps.payload[0];
		}
		else
		{
			logger.info("Py engine is not available anymore ");
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
		}
	}

	
	@Override
	protected synchronized Hashtable executePyDirect(String...script)
	{
		Hashtable retHash = new Hashtable();
		retHash.put(script, runScript(script[0])); 
		return retHash;
	}

	@Override
	protected void executeEmptyPyDirect(String script, Insight in)
	{
		
		runScript(script, in);
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
