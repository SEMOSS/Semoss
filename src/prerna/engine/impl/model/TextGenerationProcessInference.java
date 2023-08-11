package prerna.engine.impl.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;


import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TextGenerationProcessInference extends AbstractModelEngine {
	
	private static Logger logger = LogManager.getLogger(TextGenerationProcessInference.class);
	private HashMap<String,String> launchArguments = new HashMap<String,String>();
	private Process process;
    private String workerAddress;
    private String controllerAddress;
    private String inferencePort;
    private ImmutableMap<String, String> possibleInputs = getLauncherArgs();
	
	@Override
	public void loadModel(String modelSmssFilePath) {
		try {
			if (modelSmssFilePath != null) {
				logger.info("Loading Model - " + Utility.cleanLogString(FilenameUtils.getName(modelSmssFilePath)));
				setSmssFilePath(modelSmssFilePath);
				setSmssProp(Utility.loadProperties(modelSmssFilePath));
			}
			for (String launcherArg : possibleInputs.keySet()) {
				String propArg = (String) generalEngineProp.get(launcherArg);
				if(propArg != null && !propArg.isEmpty()){
					this.launchArguments.put(launcherArg,propArg);
				}
			}
	        if (!this.launchArguments.containsKey(Constants.MODEL)) {
	        	throw new IllegalArgumentException("Model name is a required argument.");
	        } 
	        
	        if (!this.launchArguments.containsKey("PORT")) {
	        	this.inferencePort=Utility.findOpenPort();
	        	launchArguments.put("PORT", this.inferencePort);
	        } else {
	        	this.inferencePort=this.launchArguments.get("PORT");
	        }
			
			
			this.workerAddress = DIHelper.getInstance().getProperty(Constants.WORKER_ADDRESS);
			if (this.workerAddress == null || this.workerAddress.trim().isEmpty()) {
				this.workerAddress = System.getenv(Constants.WORKER_ADDRESS);
			}
			if (this.controllerAddress ==null || this.controllerAddress.trim().isEmpty()) {
				this.controllerAddress = this.workerAddress + this.inferencePort;
			}
			
			String initCommand;
			if (this.generalEngineProp.containsKey("ENDPOINT")) {
				initCommand = String.format((String) this.generalEngineProp.get(Constants.INIT_MODEL_ENGINE),(String) this.generalEngineProp.get("ENDPOINT"));
			} else {
				initCommand = String.format((String) this.generalEngineProp.get(Constants.INIT_MODEL_ENGINE), this.controllerAddress);
			}
			this.generalEngineProp.put(Constants.INIT_MODEL_ENGINE, initCommand);
			
			// create a generic folder
			this.workingDirecotry = "EM_MODEL_" + Utility.getRandomString(6);
			this.workingDirectoryBasePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + workingDirecotry;
			this.cacheFolder = new File(workingDirectoryBasePath);
			
			// make the folder if one does not exist
			if(!cacheFolder.exists())
				cacheFolder.mkdir();

		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to load model details from the SMSS file");
		}
	}
	

	@Override
	public void startServer() {
		List<String> command = new ArrayList<>();
		if (this.launchArguments.containsKey(Constants.GPU_ID)) {
    		command.add(possibleInputs.get(Constants.GPU_ID));
        	command.add(this.launchArguments.get(Constants.GPU_ID));
        	this.launchArguments.remove(Constants.GPU_ID);
		}
		command.add("text-generation-launcher");
		for (String arg : this.launchArguments.keySet()) {
    		command.add(possibleInputs.get(arg));
        	command.add(this.launchArguments.get(arg));
		}
		
		System.out.println("Executing command: " + String.join(" ", command));
		
		ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.inheritIO(); // Redirect the subprocess's standard error and output to the current process
       
        try {
			this.process = processBuilder.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        // Wait for the process to finish loading
        // Read the output of the process
        String logPattern = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}Z  INFO.*Connected";
        Pattern pattern = Pattern.compile(logPattern);
        // Read the output log file in real-time
        try (InputStream inputStream = process.getInputStream();
        		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Check if the line matches the log pattern
            	logger.info(line);
                Matcher matcher = pattern.matcher(line);
                if (matcher.find() || line.endsWith("Connected")) {
                    // Process the matching log line, if needed
                	logger.info("Process has finished loading");
                    break;
                }
            }
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        super.startServer();
	}
	
	@Override
	public void stopModel() {
        if (process != null) {
            // Attempt to gracefully shut down the Python process first
            process.destroy();

            try {
                // Create a separate thread to wait for the process to exit with a timeout of 5 seconds
                Thread waitThread = new Thread(() -> {
                    try {
                        process.waitFor();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

                // Start the thread to wait for the process
                waitThread.start();

                // Wait for the thread to complete with a timeout of 5 seconds
                waitThread.join(5000);

                // Optionally, you can try to forcibly terminate the process if it's still running after graceful shutdown
                if (process.isAlive()) {
                    process.destroyForcibly();
                    System.out.println("Process forcefully terminated.");
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        super.stopModel();
	}
	

	@Override
	public String askQuestion(String question, String context, Insight insight, Map<String, Object> parameters) {
		
		String varName = (String) generalEngineProp.get("VAR_NAME");
		
		StringBuilder callMaker = new StringBuilder().append(varName).append(".ask(");
		callMaker.append("question=\"").append(question).append("\"");
		if(context != null)
			callMaker.append(",").append("context=").append(context);
		
		
		if(parameters != null) {
			if (parameters.containsKey("ROOM_ID")) { //always have to remove roomId so we dont pass it to py client
				String roomId = (String) parameters.get("ROOM_ID");
				parameters.remove("ROOM_ID");
				if (Utility.isModelInferenceLogsEnabled()) { // have to check that inference logs are enabled so that query works
					String history = getConversationHistory(roomId);
					if(history != null) //could still be null if its the first question in the convo
						callMaker.append(",").append("history=").append(history);
				}
			}
						
			Iterator <String> paramKeys = parameters.keySet().iterator();
			while(paramKeys.hasNext()) {
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
		System.out.println(callMaker.toString());
		Object output = pyt.runScript(callMaker.toString());
		return output+"";
	}
	

    private static ImmutableMap<String, String> getLauncherArgs() {
    	return new ImmutableMap.Builder<String, String>()
    			.put(Constants.GPU_ID,"CUDA_VISIBLE_DEVICES")
    			.put(Constants.MODEL,"--model-id")
    			.put("REVISION","--revision")
    			.put("SHARDED","--sharded")
    			.put(Constants.NUM_GPU,"--num-shard")
    			.put("QUANTIZE","--quantize")
    			.put("TRUST_REMOTE_CODE","--trust-remote-code")
    			.put("MAX_CONCURRENT_REQUESTS","--max-concurrent-requests")
    			.put("MAX_BEST_OF","--max-best-of")
    			.put("MAX_STOP_SEQUENCES","--max-stop-sequences")
    			.put("MAX_INPUT_LENGTH","--max-input-length")
    			.put("MAX_TOTAL_TOKENS","--max-total-tokens")
    			.put("MAX_BATCH_SIZE","--max-batch-size")
    			.put("WAITING_SERVED_RATIO","--waiting-served-ratio")
    			.put("MAX_BATCH_TOTAL_TOKENS","--max-batch-total-tokens")
    			.put("MAX_WAITING_TOKENS","--max-waiting-tokens")
    			.put("PORT","--port")
    			.put("SHARD_UDS_PATH","--shard-uds-path")
    			.put("MASTER_ADDR","--master-addr")
    			.put("MASTER_PORT","--master-port")
    			.put("HUGGINGFACE_HUB_CACHE","--huggingface-hub-cache")
    			.put("WEIGHTS_CACHE_OVERRIDE","--weights-cache-override")
    			.put("DISABLE_CUSTOM_KERNERLS","--disable-custom-kernels")
    			.put("JSON_OUTPUT","--json-output")
    			.put("OTLP_ENDPOINT","--otlp-endpoint")
    			.put("CORS_ALLOW_ORIGIN","--cors-allow-origin")
    			.put("WATERMARK_GAMMA","--watermark-gamma")
    			.put("WATERMARK_DELTA","--watermark-delta")
    			.build();
    }
}
