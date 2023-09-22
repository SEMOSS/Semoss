//package prerna.cluster.util.clients;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.lang.ProcessBuilder.Redirect;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.stream.Collectors;
//
//import prerna.cluster.util.ClusterUtil;
//import prerna.project.api.IProject;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//import prerna.util.sql.RdbmsTypeEnum;
//
//public abstract class AbstractCloudClient implements ICloudClient {
//
//	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
//	protected static final String SMSS_POSTFIX = "-smss";
//
//	protected String dbFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.DB_FOLDER;
//	protected String projectFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.PROJECT_FOLDER;
//	protected String userFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + Constants.USER_FOLDER;
//	
//	protected String RCLONE = "rclone";
//	protected String rcloneConfigFolder = null;
//	protected String TRANSFER_LIMIT = "8";
//	// this must be set in the implementing class
//	protected String PROVIDER = null;
//	
//	private static AbstractCloudClient singleton = null;
//	
//	public AbstractCloudClient(ICloudClientBuilder builder) {
//		// used to enforce builder for creation of cloud clients
//		this.RCLONE = builder.getRClonePath();
//	}
//	
//	/**
//	 * Get the cloud client for this cluster
//	 * @return
//	 */
//	@Deprecated
//	public static AbstractCloudClient getClient() {
//		if(singleton != null) {
//			return singleton;
//		}
//		
//		if(singleton == null) {
//			synchronized (AbstractCloudClient.class) {
//				if(singleton == null) {
//					singleton = buildClient();
//				}
//			}
//		}
//		
//		return singleton;
//	}
//	
//	private static synchronized AbstractCloudClient buildClient() {
//		if(ClusterUtil.STORAGE_PROVIDER == null||ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AZURE")){
//			return new AZClientBuilder().pullValuesFromSystem().buildClient();
//		}
//		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("AWS")||ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("S3")){
//			return new S3ClientBuilder().pullValuesFromSystem().buildClient();
//		} 
//		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("MINIO")){
//			AbstractCloudClient client = new MinioClientBuilder().pullValuesFromSystem().buildClient();
//			client.TRANSFER_LIMIT = "4";
//			return client;
//		} 
//		else if(ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("GCP")||ClusterUtil.STORAGE_PROVIDER.equalsIgnoreCase("GOOGLE")){
//			return new GCPClientBuilder().pullValuesFromSystem().buildClient();
//		}
//		else {
//			throw new IllegalArgumentException("You have specified an incorrect storage provider");
//		}
//	}
//
//	/**
//	 * Abstract method to create the r clone config
//	 * @return
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	protected abstract String createRcloneConfig() throws IOException, InterruptedException;
//	
//	/**
//	 * 
//	 * @param rcloneConfig
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	protected void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
//		String configPath = getConfigPath(rcloneConfig);
//		try {
//			runRcloneProcess(rcloneConfig, "rclone", "config", "delete", rcloneConfig);
//		} finally {
//			new File(configPath).delete();
//		}
//	}
//
//	/**
//	 * 
//	 * @param rcloneConfig
//	 * @param command
//	 * @return
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	protected List<String> runRcloneProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
//		String configPath = getConfigPath(rcloneConfig);
//		List<String> commandList = new ArrayList<>();
//		commandList.addAll(Arrays.asList(command));
//		commandList.add("--config");
//		commandList.add(configPath);
//		commandList.add("--fast-list");
//		String[] newCommand = commandList.toArray(new String[] {});
//		return runAnyProcess(newCommand);	
//	}
//	
//	/**
//	 * 
//	 * @param rcloneConfig
//	 * @param command
//	 * @return
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	protected List<String> runRcloneTransferProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
//		String configPath = getConfigPath(rcloneConfig);
//		List<String> commandList = new ArrayList<>();
//		commandList.addAll(Arrays.asList(command));
//		commandList.add("--transfers");
//		commandList.add(TRANSFER_LIMIT);
//		commandList.add("--config");
//		commandList.add(configPath);
//		commandList.add("--fast-list");
//		String[] newCommand = commandList.toArray(new String[] {});
//		return runAnyProcess(newCommand);	
//	}
//	
//	/**
//	 * 
//	 * @param rcloneConfig
//	 * @param command
//	 * @return
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	protected List<String> runRcloneDeleteFileProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
//		String configPath = getConfigPath(rcloneConfig);
//		List<String> commandList = new ArrayList<>();
//		commandList.addAll(Arrays.asList(command));
//		commandList.add("--config");
//		commandList.add(configPath);
//		String[] newCommand = commandList.toArray(new String[] {});
//		return runAnyProcess(newCommand);	
//	}
//
//	/**
//	 * 
//	 * @param rcloneConfig
//	 * @return
//	 */
//	protected String getConfigPath(String rcloneConfig) {
//		if( rcloneConfigFolder == null) {
//			rcloneConfigFolder =  DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "rcloneConfig";
//			new File(rcloneConfig).mkdirs();
//		}
//		
//		return rcloneConfigFolder + FILE_SEPARATOR + rcloneConfig + ".conf";
//	}
//
//	/**
//	 * 
//	 * @param command
//	 * @return
//	 * @throws IOException
//	 * @throws InterruptedException
//	 */
//	protected static List<String> runAnyProcess(String... command) throws IOException, InterruptedException {
//		// Need to allow this process to execute the below commands
//		SecurityManager priorManager = System.getSecurityManager();
//		System.setSecurityManager(null);
//		try {
//			Process p = null;
//			try {
//				ProcessBuilder pb = new ProcessBuilder(command);
//				pb.directory(new File(Utility.normalizePath(System.getProperty("user.home"))));
//				pb.redirectOutput(Redirect.PIPE);
//				pb.redirectError(Redirect.PIPE);
//				p = pb.start();
//				p.waitFor();
//				List<String> results = streamOutput(p.getInputStream());
//				streamError(p.getErrorStream());
//				return results;
//			} finally {
//				if (p != null) {
//					p.destroyForcibly();
//				}
//			}
//		} finally {
//			System.setSecurityManager(priorManager);
//		}
//	}
//	
//	/**
//	 * 
//	 * @param stream
//	 * @return
//	 * @throws IOException
//	 */
//	protected static List<String> streamOutput(InputStream stream) throws IOException {
//		return stream(stream, false);
//	}
//
//	/**
//	 * 
//	 * @param stream
//	 * @return
//	 * @throws IOException
//	 */
//	protected static List<String> streamError(InputStream stream) throws IOException {
//		return stream(stream, true);
//	}
//
//	/**
//	 * 
//	 * @param stream
//	 * @param error
//	 * @return
//	 * @throws IOException
//	 */
//	protected static List<String> stream(InputStream stream, boolean error) throws IOException {
//		try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
//			List<String> lines = reader.lines().collect(Collectors.toList());
//			for(String line : lines) {
//				if (error) {
//					System.err.println(line);
//				} else {
//					System.out.println(line);
//				}
//			}
//			return lines;
//		}
//	}
//	
//	/**
//	 * 
//	 * @param appFolder
//	 * @return
//	 */
//	protected List<String> getSqlLiteFile(String appFolder) {
//		File dir = new File(appFolder);
//		List<String> sqlFiles = new ArrayList<String>();
//		//search dir for .sqlite files 
//		for (File file : dir.listFiles()) {
//			if (file.getName().endsWith((".sqlite"))) {
//				if (!(file.getName().equals("insights_database.sqlite"))) {
//					sqlFiles.add(file.getName());
//				}
//			}
//		}
//		if (sqlFiles.size() > 1){
//			System.out.println("More than 1 sqlite file found in app dir. Adding only first");
//		}
//		return sqlFiles;
//	}
//
//	/**
//	 * 
//	 * @param project
//	 * @param specificProjectFolder
//	 * @return
//	 */
//	protected String getInsightDB(IProject project, String specificProjectFolder) {
//		RdbmsTypeEnum insightDbType = project.getInsightDatabase().getDbType();
//		String insightDbName = null;
//		if (insightDbType == RdbmsTypeEnum.H2_DB) {
//			insightDbName = "insights_database.mv.db";
//		} else {
//			insightDbName = "insights_database.sqlite";
//		}
//		File dir = new File(specificProjectFolder);
//		for (File file : dir.listFiles()) {
//			if (file.getName().equalsIgnoreCase(insightDbName)){
//				return file.getName();
//			}
//		}
//		throw new IllegalArgumentException("There is no insight database for project: " + project.getProjectName());
//	}
//
//}