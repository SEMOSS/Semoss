package prerna.engine.impl.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.IModelEngine;
import prerna.om.Insight;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class EmbeddedModelEngine implements IModelEngine {

	// starts a embedded model in the same environment
	private static Logger logger = LogManager.getLogger(EmbeddedModelEngine.class);
	Properties prop = null;
	File cacheFolder = null;
	String prefix = null;
	Process p = null;
	String port = null;
	NativePySocketClient socketClient = null;
	TCPPyTranslator pyt = null;
	Map vars = new HashMap();
	String varName = null;
	String stopper = null;
	
	public EmbeddedModelEngine()
	{
		vars.put(Settings.VAR_NAME, "");
		vars.put(Settings.PROMPT_STOPPER, "");
		
	}
	@Override
	public void loadModel(String modelSmss) 
	{
		// TODO Auto-generated method stub
		// starts the model
		try {
			prop = new Properties();
			File file = new File(modelSmss);
			
			prop.load(new FileInputStream(file));
			genVars();
			String curDir = file.getParent();
			curDir = curDir.replace("\\", "/");
			prop.put("CUR_DIR", curDir);
			String engineName = prop.getProperty(Constants.ENGINE_ALIAS);
			String engineID = prop.getProperty(Constants.ENGINE);
			prop.put("ENGINE_DIR", curDir + "/" + engineName + "__" + engineID);
			vars = new HashMap(prop);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void startServer() {
		// TODO Auto-generated method stub
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands
		String initCommands = (String)prop.get(Constants.INIT_MODEL_ENGINE);
		
		// break the commands seperated by ;
		String [] commands = initCommands.split(";");
		// replace the Vars
		for(int commandIndex = 0;commandIndex < commands.length;commandIndex++)
			commands[commandIndex] = fillVars(commands[commandIndex]);
		
		port = Utility.findOpenPort();
		// create a generic folder
		String folderName = "EM_MODEL_" + Utility.getRandomString(6);
		String fullFolder = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + folderName;
		cacheFolder = new File(fullFolder);
		
		// make the folder if one does not exist
		if(!cacheFolder.exists())
			cacheFolder.mkdir();
			
		Object [] outputs = Utility.startTCPServerNativePy(fullFolder, port);
		this.p = (Process)outputs[0];
		this.prefix = (String)outputs[1];
		
		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		connectClient();
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setClient(socketClient);
		
		
		pyt.runEmptyPy(commands);		
	}

	//@Override
	public PyTranslator getClient() {
		// TODO Auto-generated method stub
		return this.pyt;
	}

	@Override
	public void stopModel() {
		// TODO Auto-generated method stub
		try {
			socketClient.crash();
			FileUtils.deleteDirectory(cacheFolder);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public boolean connectClient()
	{
		Thread t = new Thread(socketClient);
		t.start();
		while(!socketClient.isReady())
		{
			synchronized(socketClient)
			{
				try 
				{
					socketClient.wait();
					logger.info("Setting the socket client ");
				} catch (InterruptedException e) {
					logger.error(Constants.STACKTRACE, e);
				}								
			}
		}

		return false;
	}

	@Override
	public String ask(String question, String context, Insight insight, Map <String, Object> parameters) 
	{
		// TODO Auto-generated method stub
		//if(this.pyt == null)
		//	return null;
		if(!this.socketClient.isConnected())
			this.startServer();
		
		String varName = (String)prop.get(Settings.VAR_NAME);
		
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question='").append(question).append("'");
		if(context != null)
			callMaker.append(",").append("context='").append(context).append("'");
	
		if(parameters != null)
		{
			Iterator <String> paramKeys = parameters.keySet().iterator();
			while(paramKeys.hasNext())
			{
				String key = paramKeys.next();
				callMaker.append(",").append(key).append("=");
				Object value = parameters.get(key);
				if(value instanceof String)
				{
					callMaker.append("'").append(value+"").append("'");
				}
				else
				{
					callMaker.append(value+"");
				}
			}
		}
		callMaker.append(")");
		System.err.println("call maker.. " + callMaker);
		
		Object output = pyt.runScript(callMaker.toString(), insight);
		
		return output+"";
	}
	
	private void cleanup()
	{
		// remove the directory
		// force drop it
		
	}
	
	@Override
	public MODEL_TYPE getModelType() {
		return MODEL_TYPE.EMBEDDED;
	}
	
	@Override
	public String getCatalogType(Properties smssProp) {
		return IModelEngine.CATALOG_TYPE;
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getModelType().toString();
	}
	
	
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////

	

	@Override
	public void setEngineId(String engineId) {
		// TODO Auto-generated method stub
		
	}
	
	private void genVars()
	{
		Iterator <String> keys = vars.keySet().iterator();
		while(keys.hasNext())
		{
			String key = keys.next();
			if(!prop.containsKey(key))
			{
				String randomString = "v_" + Utility.getRandomString(6);
				prop.put(key, randomString);
			}
		}
	}
	
	private String fillVars(String input)
	{
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}

	@Override
	public String getEngineId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setEngineName(String engineName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getEngineName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSmssFilePath(String smssFilePath) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSmssFilePath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSmssProp(Properties smssProp) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Properties getSmssProp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Properties getOrigSmssProp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void delete() {
		// TODO Auto-generated method stub
		
	}
	
	public static void main(String [] args)
	{
		String propFile = "c:/users/pkapaleeswaran/workspacej3/SemossDev/db/PolicyBot.smss";
		
		DIHelper.getInstance().loadCoreProp("c:/users/pkapaleeswaran/workspacej3/SemossDev/RDF_MAP.prop");
		
		IModelEngine eng = new EmbeddedModelEngine();
		eng.loadModel(propFile);
		eng.startServer();
		
		Map <String, Object> params = new HashMap<String, Object>();
		params.put("max_new_tokens", 200);
		params.put("temperature", 0.01);
		
		String output = eng.ask("What is the capital of India ?", null, null, params);
		
		//PyTranslator pyt = eng.getClient();
		
		System.err.println(output);
		
		//Object output = pyt.runScript("i.ask(question='What is the capital of India ?')");
		
		//System.err.println(output);
	}

	
}
