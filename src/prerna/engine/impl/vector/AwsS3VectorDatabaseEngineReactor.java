package prerna.engine.impl.vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AwsS3VectorDatabaseEngineReactor extends AbstractReactor  {
	
	public AwsS3VectorDatabaseEngineReactor() {
		
	}
	@Override
	public NounMetadata execute() {

	        String accessKey = "";
	        String secretKey = "";

	        String pythonScript = "D:\\AWS\\test_insert_vector.py";

	        try {
	            ProcessBuilder processBuilder = new ProcessBuilder("D:\\Users\\shmahure\\.pyenv\\pyenv-win\\versions\\3.11.9\\python.exe", pythonScript, accessKey, secretKey);
	            processBuilder.redirectErrorStream(true);
	            Process process = processBuilder.start();

	            // Print output from the script
	            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
	            String line;
	            while ((line = reader.readLine()) != null) {
	                System.out.println(line);
	            }

	            int exitCode = process.waitFor();
	            if (exitCode == 0) {
	                System.out.println("Python script executed successfully.");
	            } else {
	                System.err.println("Python script exited with code " + exitCode);
	            }

	        } catch (IOException | InterruptedException e) {
	            e.printStackTrace();
	        }
			return null;
	}
}




	
	


