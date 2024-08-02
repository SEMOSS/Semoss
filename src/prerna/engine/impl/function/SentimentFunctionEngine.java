package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.internal.LinkedTreeMap;

import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.PortAllocator;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.util.PythonUtils;

public class SentimentFunctionEngine extends AbstractFunctionEngine2 {

	private static final Logger classLogger = LogManager.getLogger(SentimentFunctionEngine.class);

	TCPPyTranslator pyt = null;
	NativePySocketClient socketClient = null;
	Process p = null;
	String port = null;
	String workingDirectory;
	String prefix = null;
	String workingDirectoryBasePath = null;
	File cacheFolder;
	String varName = null;
	
	public SentimentFunctionEngine()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.INPUT.getKey(), ReactorKeysEnum.MIN.getKey(), ReactorKeysEnum.MAX.getKey()};
		this.keyRequired = new int[] {1, 0, 0};
	}
	
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// input - is the input - typically this is an array
		getVarName();
		List <String> inputStrings = getInput();
		
		
		StringBuilder inputs = new StringBuilder("[");
		for(int inputIndex = 0;inputIndex < inputStrings.size();inputIndex++)
		{
			if(inputIndex != 0)
				inputs.append(", ");
			inputs.append("'").append(inputStrings.get(inputIndex)).append("'");
		}
		inputs.append("]");
		
		
		StringBuilder cmd = new StringBuilder(varName).append(".execute(input_arr=" + inputs + ")");
		List output = (List)pyt.runScript(cmd.toString());
		
		// tbd implement filtering based on values
		float minValue = -1;
		float maxValue = 1;
		if(smssProp.containsKey(keysToGet[1]))
			minValue = Float.parseFloat(smssProp.getProperty(keysToGet[1]));

		if(smssProp.containsKey(keysToGet[2]))
			maxValue = Float.parseFloat(smssProp.getProperty(keysToGet[2]));
		
		// try to see if they passed as min value and max value
		if (store.getNoun(keysToGet[1]) != null)
			minValue = (float)store.getNoun(keysToGet[1]).get(0);
		
		if (store.getNoun(keysToGet[2]) != null)
			maxValue = (float)store.getNoun(keysToGet[2]).get(0);
		
		for(int outputIndex = 0;outputIndex < output.size();outputIndex++)
		{
			LinkedTreeMap<String, Object> item = (LinkedTreeMap<String, Object>) output.get(outputIndex);
			float compound = ((Double)item.get("compound")).floatValue();
			boolean approved = compound >= minValue && compound <= maxValue;
			item.put("approved", approved);
			item.put("input", inputStrings.get(outputIndex));
		}
		
		return new NounMetadata(output, PixelDataType.VECTOR);
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	
	@Override
	public void open(String smssFilePath) throws Exception 
	{
		super.open(smssFilePath);
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands
		String initCommands = (String) smssProp.get(Constants.INIT_MODEL_ENGINE);
		
		// break the commands seperated by ;
		String [] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		port = PortAllocator.getInstance().getNextAvailablePort()+"";
		
		String timeout = "15";
		if(smssProp.containsKey(Constants.IDLE_TIMEOUT))
			timeout = smssProp.getProperty(Constants.IDLE_TIMEOUT);

		if (this.workingDirectoryBasePath == null) {
			this.createCacheFolder();
		}

		String venvEngineId = this.smssProp.getProperty(Constants.VIRTUAL_ENV_ENGINE, null);
		String venvPath = venvEngineId != null ? Utility.getVenvEngine(venvEngineId).pathToExecutable() : null;
		
		String loggerLevel = this.smssProp.getProperty(Settings.LOGGER_LEVEL, "WARNING");
		Object [] outputs = PythonUtils.startTCPServerNativePy(this.workingDirectoryBasePath, port, venvPath, timeout, loggerLevel);
		this.p = (Process) outputs[0];
		this.prefix = (String) outputs[1];
		
		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		connectClient();
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setSocketClient(socketClient);
		pyt.runEmptyPy(commands);	
		
		// run a prefix command
		setPrefix(this.prefix);
	}
	
	private void getVarName()
	{
		if(smssProp != null && varName == null)
		{
			varName = smssProp.getProperty(Settings.VAR_NAME);
		}
	}
	
	private void createCacheFolder() {
		// create a generic folder
		this.workingDirectory = "FUNCTION_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + this.workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);
		
		// make the folder if one does not exist
		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
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
					classLogger.info("Setting the socket client ");
				} catch (InterruptedException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}								
			}
		}
		return false;
	}

	private void setPrefix(String prefix)
	{
		String [] alldata = new String[] {"prefix", prefix};
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = alldata;
		prefixPayload.operation = PayloadStruct.OPERATION.CMD;
		PayloadStruct ps = (PayloadStruct)socketClient.executeCommand(prefixPayload);
	}
	
	private List<String> getInput() {
		List<String> columns = new Vector<String>();

		GenRowStruct colGrs = this.store.getNoun(this.keysToGet[0]);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		} else {
			GenRowStruct inputsGRS = this.getCurRow();
			// keep track of selectors to change to upper case
			if (inputsGRS != null && !inputsGRS.isEmpty()) {
				for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
					String column = inputsGRS.get(selectIndex) + "";
					columns.add(column);
				}
			}
		}

		return columns;
	}


}
