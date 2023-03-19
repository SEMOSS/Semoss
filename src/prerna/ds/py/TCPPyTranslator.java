package prerna.ds.py;

import java.io.File;
import java.util.Hashtable;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.ErrorSenderThread;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TCPPyTranslator extends PyTranslator {

	public static final String METHOD_DELIMITER = "$$##";
	public SocketClient nc = null;
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

			/*
			if(ps.payload != null && ps.payload.length > 0 && (ps.payload[0].toString().startsWith("smssutil.runwrappereval(") || ps.payload[0].toString().startsWith("smssutil.runwrapper(")) )
			{
				// really dirty manipulation of payload since the thread doesnt allow calling across threads from python
				// smssutil.runwrappereval("C:/users/pkapaleeswaran/workspacej3/SemossDev/InsightCache/D51AF71E6313E7516C716491C2C83AE9/caf28ff2-de29-4453-abd9-42e48f59a088/Py/Temp/av1kc8xQE1SzC.py", "C:/users/pkapaleeswaran/workspacej3/SemossDev/InsightCache/D51AF71E6313E7516C716491C2C83AE9/caf28ff2-de29-4453-abd9-42e48f59a088/Py/Temp/av1kc8xQE1SzC.txt", "C:/users/pkapaleeswaran/workspacej3/SemossDev/InsightCache/D51AF71E6313E7516C716491C2C83AE9/caf28ff2-de29-4453-abd9-42e48f59a088/Py/Temp/av1kc8xQE1SzC.txt", globals())
				// it is already being set.. nothing to do
//				String thisPayload = ps.payload[0]+"";
//				thisPayload = thisPayload.substring(thisPayload.indexOf("("));
//				String [] payloadParts = thisPayload.split(",");
//				if(payloadParts.length == 4) // this is our guy
//				{
//					file = payloadParts[1];
//					System.err.println("File is " + file);
//					//et.epoc = ps.epoc;
//					// I think we can simplify this.. but for now
//					est.setFile(file);
//					est.start();
//				}
			}
			else
			{*/
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
			//}
		}
		
		
		if(nc.isConnected())
		{
			ps = (PayloadStruct)nc.executeCommand(ps);
			if(est != null)
				est.stopSession();
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
