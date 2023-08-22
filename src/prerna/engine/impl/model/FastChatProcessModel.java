package prerna.engine.impl.model;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import prerna.engine.api.ModelTypeEnum;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class FastChatProcessModel extends AbstractModelEngine {
	
    private String workerAddress;
    private String controllerAddress;
    private String modelPath;
    private String gpuIdString;
    private String gpuNum;
    private String port; 
    private long pid;
    private Process process;
    
	@Override
	public void open(String smssFilePath) {
		// TODO Auto-generated method stub
		// starts the model
		try {
			generalEngineProp = new Properties();
			File file = new File(smssFilePath);
			
			generalEngineProp.load(new FileInputStream(file));
			
			workerAddress = DIHelper.getInstance().getProperty(Constants.WORKER_ADDRESS);
			if(workerAddress ==null || workerAddress.trim().isEmpty()) {
	            workerAddress = System.getenv(Constants.WORKER_ADDRESS);
			}
			controllerAddress = DIHelper.getInstance().getProperty(Constants.CONTROLLER_ADDRESS);
			if(controllerAddress ==null || controllerAddress.trim().isEmpty()) {
				controllerAddress = System.getenv(Constants.WORKER_ADDRESS);
			}
			modelPath = (String)generalEngineProp.get(Constants.MODEL);
			//TODO comes from some gpu client on resources
			gpuIdString = (String)generalEngineProp.get(Constants.GPU_ID);
			gpuNum = (String)generalEngineProp.get(Constants.NUM_GPU);


		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
			e.printStackTrace();
		}
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
