package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import prerna.util.Constants;
import prerna.util.DIHelper;

public abstract class CloudClient {

	private static final String FILE_SEPARATOR = System.getProperty("file.separator");
	static String rcloneConfigFolder = null;

	public static CloudClient getClient(){

		if(ClusterUtil.STORAGE_PROVIDER == null){
			return AZClient.getInstance();
		}
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AZURE")){
			return AZClient.getInstance();
		}
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AWS")){

			//add in aws 
			return AWSClient.getInstance();
		}
		else{
			throw new IllegalArgumentException("You have specified an incorrect storage provider");
		}
	}

	public abstract void init();

	public  abstract void pushApp(String appId) throws IOException, InterruptedException;

	public  abstract void pullApp(String appId) throws IOException, InterruptedException;

	protected abstract void pullApp(String appId, boolean newApp) throws IOException, InterruptedException; 

	public abstract void updateApp(String appId) throws IOException, InterruptedException;

	public abstract void deleteApp(String appId) throws IOException, InterruptedException;

	public abstract List<String> listAllBlobContainers() throws IOException, InterruptedException; 

	public abstract void deleteContainer(String containerId) throws IOException, InterruptedException; 

	public abstract void syncInsightsDB(String appId) throws IOException, InterruptedException;
	
	protected static void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "delete", rcloneConfig);
		} finally {
			new File(configPath).delete();
		}
	}

	protected static List<String> runRcloneProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);	
	}

	protected static String getConfigPath(String rcloneConfig) {

		if( rcloneConfigFolder == null) {
			rcloneConfigFolder =  DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "rcloneConfig";		
		}
		
		return rcloneConfigFolder + FILE_SEPARATOR + rcloneConfig + ".conf";
	}

	protected static List<String> runAnyProcess(String... command) throws IOException, InterruptedException {
		Process p = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(command);
			pb.directory(new File(System.getProperty("user.home")));
			pb.redirectOutput(Redirect.PIPE);
			pb.redirectError(Redirect.PIPE);
			p = pb.start();
			p.waitFor();
			List<String> results = streamOutput(p.getInputStream());
			streamError(p.getErrorStream());
			return results;
		} finally {
			if (p != null) {
				p.destroyForcibly();
			}
		}
	}
	

	protected static List<String> streamOutput(InputStream stream) throws IOException {
		return stream(stream, false);
	}

	protected static List<String> streamError(InputStream stream) throws IOException {
		return stream(stream, true);
	}

	protected static List<String> stream(InputStream stream, boolean error) throws IOException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
			List<String> lines = reader.lines().collect(Collectors.toList());
			for(String line : lines) {
				if (error) {
					System.err.println(line);
				} else {
					System.out.println(line);
				}
			}
			return lines;
		}
	}


}

