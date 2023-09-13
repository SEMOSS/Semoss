package prerna.engine.impl.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ModelTypeEnum;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class FastChatProcessModel extends AbstractModelEngine {
	
	private static Logger classLogger = LogManager.getLogger(FastChatProcessModel.class);
	
    private String workerAddress;
    private String controllerAddress;
    private String modelPath;
    private String gpuIdString;
    private String gpuNum;
    private String port; 
    private long pid;
    private Process process;
    
	@Override
	public void open(Properties smssProp) throws Exception {
		setSmssProp(smssProp);
		
		// WHY IS THIS DIHELPER AND NOT IN THE SMSS FILE????
		workerAddress = DIHelper.getInstance().getProperty(Constants.WORKER_ADDRESS);
		if(workerAddress ==null || workerAddress.trim().isEmpty()) {
            workerAddress = System.getenv(Constants.WORKER_ADDRESS);
		}
		controllerAddress = DIHelper.getInstance().getProperty(Constants.CONTROLLER_ADDRESS);
		if(controllerAddress == null || controllerAddress.trim().isEmpty()) {
			controllerAddress = System.getenv(Constants.WORKER_ADDRESS);
		}
		
		
		modelPath = this.smssProp.getProperty(Constants.MODEL);
		//TODO comes from some gpu client on resources
		gpuIdString = this.smssProp.getProperty(Constants.GPU_ID);
		gpuNum = this.smssProp.getProperty(Constants.NUM_GPU);
	}

    public void startServer()  {
    	this.port=Utility.findOpenPort();
        List<String> command = new ArrayList<>();
        command.add("python3");
        command.add("-m");
        command.add("fastchat.serve.model_worker");
        command.add("--model-path");
        command.add(modelPath);
        command.add("--gpus");
        command.add(gpuIdString);
        command.add("--num-gpus");
        command.add(gpuNum);
        command.add("--worker-address");
        command.add(workerAddress);
        command.add("--controller-address");
        command.add(controllerAddress);
        command.add("--host");
        command.add("0.0.0.0");
        command.add("--port");
        command.add(port.toString());

        System.out.println("Executing command: " + String.join(" ", command));

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.inheritIO(); // Redirect the subprocess's standard error and output to the current process
        
         // Set CUDA_VISIBLE_DEVICES environment variable
        if (gpuIdString != null && !gpuIdString.isEmpty()) {
            processBuilder.environment().put("CUDA_VISIBLE_DEVICES", gpuIdString);
        }
        try {
			process = processBuilder.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
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
    }

	@Override
	public String askQuestion(String question, String context, Insight insight, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.FAST_CHAT;
	}
}
