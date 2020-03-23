package prerna.ds.py;

import java.util.Hashtable;

import prerna.pyserve.NettyClient;

public class TCPPyTranslator extends PyTranslator {

	public NettyClient nc = null;
	
	@Override
	public synchronized Object runScript(String script) 
	{
		//System.out.println(".");
		Object response = nc.executeCommand(script);
		//Object [] outputObj = (Object [])response;
		
		//System.out.println("Command was " + outputObj[0] + "<>" + script + "<>" + outputObj[1]);
		//System.err.println("Got the response back !!!!! WOO HOO " + response);
		return response;
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


	
}
