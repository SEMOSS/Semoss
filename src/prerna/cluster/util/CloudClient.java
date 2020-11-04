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
import java.util.stream.Collectors;

import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public abstract class CloudClient {

	private static String TRANSFER_LIMIT = "8";
	static String rcloneConfigFolder = null;
	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/**
	 * Get the cloud client for this cluster
	 * @return
	 */
	public static CloudClient getClient(){
		if(ClusterUtil.STORAGE_PROVIDER == null){
			return AZClient.getInstance();
		}
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AZURE")){
			return AZClient.getInstance();
		}
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AWS")||ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("S3")){
			return S3Client.getInstance();
		} 
		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("MINIO")){
			TRANSFER_LIMIT = "4";
			return S3Client.getInstance();
		} 
		else{
			throw new IllegalArgumentException("You have specified an incorrect storage provider");
		}
	}

	public abstract void init();

	public  abstract void pushApp(String appId) throws IOException, InterruptedException;
	
	public  abstract void pushDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException;


	public  abstract void pullApp(String appId) throws IOException, InterruptedException;
	
	public  abstract void pullDB(String appId, RdbmsTypeEnum e) throws IOException, InterruptedException;

	protected abstract void pullApp(String appId, boolean appAlreadyLoaded) throws IOException, InterruptedException; 
	
	public  abstract void pullImageFolder() throws IOException, InterruptedException;

	public  abstract void pushImageFolder() throws IOException, InterruptedException;

	public abstract void updateApp(String appId) throws IOException, InterruptedException;

	public abstract void deleteApp(String appId) throws IOException, InterruptedException;

	public abstract List<String> listAllBlobContainers() throws IOException, InterruptedException; 

	public abstract void deleteContainer(String containerId) throws IOException, InterruptedException; 

	public abstract void pullInsightsDB(String appId) throws IOException, InterruptedException;
	
	public abstract void pushInsightDB(String appId)  throws IOException, InterruptedException;

	public abstract void pushOwl(String appId)  throws IOException, InterruptedException;

	public abstract void pullOwl(String appId)  throws IOException, InterruptedException;

	public abstract void pushFolder(String appId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException;

	public abstract void pullFolder(String appId, String absolutePath, String remoteRelativePath) throws IOException, InterruptedException;

	
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
	
	protected static List<String> runRcloneTransferProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--transfers");
		commandList.add(TRANSFER_LIMIT);
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
		
		// Need to allow this process to execute the below commands
		SecurityManager priorManager = System.getSecurityManager();
		System.setSecurityManager(null);
		try {
			Process p = null;
			try {
				ProcessBuilder pb = new ProcessBuilder(command);
				pb.directory(new File(Utility.normalizePath(System.getProperty("user.home"))));
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
		} finally {
			System.setSecurityManager(priorManager);
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
	
	protected List<String> getSqlLiteFile(String appFolder) {
        File dir = new File(appFolder);
        List<String> sqlFiles = new ArrayList<String>();
        //search dir for .sqlite files 
        for (File file : dir.listFiles()) {
            if (file.getName().endsWith((".sqlite"))) {
            	if (!(file.getName().equals("insights_database.sqlite")))
            	sqlFiles.add(file.getName());
            }
          }
        if (sqlFiles.size() > 1){
        	System.out.println("More than 1 sqlite file found in app dir. Adding only first");
        }
		return sqlFiles;
	}

	protected String getInsightDB(IEngine engine, String appFolder) {
		String insightDbType = engine.getProperty(Constants.RDBMS_INSIGHTS_TYPE);
		String insightDbName = null;
		if (insightDbType.toLowerCase().contains("h2")) {
			insightDbName = "insights_database.mv.db";
		} else {
			insightDbName = "insights_database.sqlite";
		}
        File dir = new File(appFolder);
        for (File file : dir.listFiles()) {
            if (file.getName().equalsIgnoreCase(insightDbName)){
            	return file.getName();
            }
          }
		throw new IllegalArgumentException("There is no insight database for app: " + engine.getEngineName());
	}
	
}

