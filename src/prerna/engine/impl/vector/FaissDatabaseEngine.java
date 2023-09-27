package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.model.ModelEngineConstants;
import prerna.engine.impl.service.RESTServiceEngine;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class FaissDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseEngine.class);
	
	private static final String DIR_SEPERATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	protected String vectorDatabaseSearcher = null;
	
	// python server
	TCPPyTranslator pyt = null;
	NativePySocketClient socketClient = null;
	Process p = null;
	String port = null;
	String prefix = null;
	String workingDirecotry;
	String workingDirectoryBasePath;
	File cacheFolder;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		this.workingDirectoryBasePath = this.connectionURL;
		this.smssProp.put("WORKING_DIR", this.connectionURL);
		this.encoderName = this.smssProp.getProperty("ENCODER_NAME");
		this.encoderName = this.smssProp.getProperty("ENCODER_TYPE");
		
		this.cacheFolder = new File(workingDirectoryBasePath + DIR_SEPERATOR + "py" + DIR_SEPERATOR);
		// make the folder if one does not exist
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
		
		this.vectorDatabaseSearcher = this.smssProp.getProperty("VECTOR_SEARCHER_NAME");
		
		// vars for string substitution
		this.vars = new HashMap<>(this.smssProp);
		
		this.startServer();
	}
	
	public void startServer() {
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands
		String initCommands = this.smssProp.getProperty(Constants.INIT_MODEL_ENGINE);
		
		
		// break the commands seperated by ;
		String [] commands = initCommands.split(ModelEngineConstants.PY_COMMAND_SEPARATOR);
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		port = Utility.findOpenPort();
		
		String timeout = "15";
		if(smssProp.containsKey(Constants.IDLE_TIMEOUT))
			timeout = smssProp.getProperty(Constants.IDLE_TIMEOUT);

		Object [] outputs = Utility.startTCPServerNativePy(this.cacheFolder.getPath(), port, timeout);
		this.p = (Process) outputs[0];
		this.prefix = (String) outputs[1];
		
		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		connectClient();
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setClient(socketClient);
		pyt.runEmptyPy(commands);	
		
		// run a prefix command
		//setPrefix(this.prefix);
	}
	
	@SuppressWarnings("unchecked")
	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}
	
	public boolean connectClient() {
		Thread t = new Thread(socketClient);
		t.start();
		while(!socketClient.isReady())
		{
			synchronized(socketClient)
			{
				try 
				{
					socketClient.wait();
					classLogger.info("Setting the socket client ");
				} catch (InterruptedException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}								
			}
		}
		return false;
	}
	
	@Override
	public void addDocumet(List <String> fileNames) throws NumberFormatException, IOException {
		// first we need to extract the text from the document
		// TODO change this to json so we never have an encoding issue
		FaissDatabaseUtils.convertFilesToCSV(this.getEngineName(), Integer.parseInt(this.smssProp.getProperty("CONTENT_LENGTH")), Integer.parseInt(this.smssProp.getProperty("CONTENT_OVERLAP")), fileNames);
		
		
		
	}
	
	@Override
	public void close() {
		try {
			socketClient.crash();
			FileUtils.deleteDirectory(cacheFolder);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (SemossPixelException e) {
			if (e.getMessage().equals("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe")){
				;
			} else {
				throw e;
			}
		}
		
		DIHelper.getInstance().removeEngineProperty(this.engineId); 
	}

	@Override
	public void removeDocument(List<String> fileNames) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object nearestNeighbor(String question, String limit) {
		
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
		
		// TODO Auto-generated method stub
		String varName = (String) smssProp.get("VECTOR_SEARCHER_NAME");
		StringBuilder callMaker = new StringBuilder().append(varName).append(".get_result_faiss(");
		callMaker.append("question=\"\"\"").append(question.replace("\"", "\\\"")).append("\"\"\"");
		if(limit != null) {
			callMaker.append(",").append("results = ").append(limit);
		}
		callMaker.append(")");
		Object output = pyt.runScript(callMaker.toString());
		return output;
	}
	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.FAISS;
	}
	
	public static void main(String[] args) throws Exception {
//		Properties tempSmss = new Properties();
//		tempSmss.put("ENGINE", "");
//		tempSmss.put("", "");
//		tempSmss.put("", "");
//		tempSmss.put("", "");
//		tempSmss.put("", "");
//		tempSmss.put("", "");
//		tempSmss.put("", "");
//		tempSmss.put("", "");
//		tempSmss.put("", "");
//		
//		tempSmss.put("URL", "http://127.0.0.1:5000/runML");
//		tempSmss.put("HTTP_METHOD", "post");
//		tempSmss.put("HEADERS", "{Content-Type: 'application/json'}");
//		tempSmss.put("EXECUTE_INPUT_NAMES", "['number1','number2']");
//		tempSmss.put("CONTENT_TYPE", "JSON");
//		FaissDatabaseEngine engine = new FaissDatabaseEngine();
//		engine.open(tempSmss);
//		
//		
//		
//		//Object output = engine.execute(new Object[] {1,2});
//		System.out.println("My output = " + output);
//		engine.close();
	}
}
