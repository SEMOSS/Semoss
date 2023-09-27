package prerna.engine.impl.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TextGenerationProcessInference extends TextGenerationEngine {
	
	private static Logger classLogger = LogManager.getLogger(TextGenerationProcessInference.class);
	
	private HashMap<String,String> launchArguments = new HashMap<String,String>();
	private Process process;
    private String workerAddress;
    private String controllerAddress;
    private String inferencePort;
    private ImmutableMap<String, String> possibleInputs = getLauncherArgs();
	
	@Override
	public void open(String smssFilePath) {
		try {
			if (smssFilePath != null) {
				classLogger.info("Loading Model - " + Utility.cleanLogString(FilenameUtils.getName(smssFilePath)));
				setSmssFilePath(smssFilePath);
				setSmssProp(Utility.loadProperties(smssFilePath));
			}
			for (String launcherArg : possibleInputs.keySet()) {
				String propArg = (String) smssProp.get(launcherArg);
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
				this.controllerAddress = this.workerAddress + ":" + this.inferencePort;
			}
			
			if (!this.smssProp.containsKey("ENDPOINT")) {
				this.smssProp.put("ENDPOINT", this.controllerAddress);
			} 
			
			// create a generic folder
			this.workingDirecotry = "EM_MODEL_" + Utility.getRandomString(6);
			this.workingDirectoryBasePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + workingDirecotry;
			this.cacheFolder = new File(workingDirectoryBasePath);
			
			// make the folder if one does not exist
			if(!cacheFolder.exists())
				cacheFolder.mkdir();
			
			// vars for string substitution
			vars = new HashMap<>(smssProp);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to load model details from the SMSS file");
		}
	}
	

	@Override
	public void startServer() {
		List<String> command = new ArrayList<>();
		if (this.launchArguments.containsKey(Constants.GPU_ID)) {
    		command.add(possibleInputs.get(Constants.GPU_ID) + "=" + this.launchArguments.get(Constants.GPU_ID));
        	this.launchArguments.remove(Constants.GPU_ID);
		}
		command.add("text-generation-launcher");
		for (String arg : this.launchArguments.keySet()) {
    		command.add(possibleInputs.get(arg));
        	command.add(this.launchArguments.get(arg));
		}
		
		System.out.println("Executing command: " + String.join(" ", command));
		
		ProcessBuilder processBuilder = new ProcessBuilder(command);
		// dont inherit IO so that we can catch 
        // processBuilder.inheritIO(); // Redirect the subprocess's standard error and output to the current process
       
        try {
			this.process = processBuilder.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
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
            	classLogger.info(line);
                Matcher matcher = pattern.matcher(line);
                if (matcher.find() || line.endsWith("Connected")) {
                    // Process the matching log line, if needed
                	classLogger.info("Process has finished loading");
                    break;
                }
            }
        } catch (IOException e) {
        	classLogger.error(Constants.STACKTRACE, e);
		}
        
        super.startServer();
	}
	
	@Override
	public void close() {
        if (process != null) {
            // Attempt to gracefully shut down the Python process first
            process.destroy();

            try {
                // Create a separate thread to wait for the process to exit with a timeout of 5 seconds
                Thread waitThread = new Thread(() -> {
                    try {
                        process.waitFor();
                    } catch (InterruptedException e) {
                        classLogger.error(Constants.STACKTRACE, e);
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
                classLogger.error(Constants.STACKTRACE, e);
            }
        }
        try {
			super.close();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
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
