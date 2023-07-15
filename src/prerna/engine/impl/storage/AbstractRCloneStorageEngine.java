package prerna.engine.impl.storage;

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
import java.util.Properties;
import java.util.stream.Collectors;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRCloneStorageEngine {

	/*
	 * 
	 * This is just for my reference while building out the engines
	 * 
		// s3 prefix is used for minio as well
		public static final String S3_REGION_KEY = "S3_REGION";
		public static final String S3_BUCKET_KEY = "S3_BUCKET";
		public static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
		public static final String S3_SECRET_KEY = "S3_SECRET_KEY";
		public static final String S3_ENDPOINT_KEY = "S3_ENDPOINT";
		
		// gcp keys
		public static final String GCP_SERVICE_ACCOUNT_FILE_KEY = "GCP_SERVICE_ACCOUNT_FILE";
		public static final String GCP_REGION_KEY = "GCP_REGION";
		public static final String GCP_BUCKET_KEY = "GCP_BUCKET";
		
		// az keys
		public static final String AZ_CONN_STRING = "AZ_CONN_STRING";
		public static final String AZ_NAME = "AZ_NAME";
		public static final String AZ_KEY = "AZ_KEY";
		public static final String SAS_URL = "SAS_URL";
		public static final String AZ_URI = "AZ_URI";
		public static final String STORAGE = "STORAGE"; // says if this is local / cluster
		public static final String KEY_HOME = "KEY_HOME"; // this is where the various keys are cycled
	 * 
	 * 
	 */
	
	protected static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	protected Properties smssProp = null;
	
	// the path to rclone executable - default assumes in path
	protected String RCLONE = "rclone";
	
	protected String rcloneConfigFolder = null;
	protected String TRANSFER_LIMIT = "8";
	
	// this must be set in the implementing class
	protected String PROVIDER = null;

	/**
	 * This method is responsible for creating the specific r clone configuration object for this storage type
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public abstract String createRcloneConfig() throws IOException, InterruptedException;
	
	/**
	 * Lists the folders and files for the relative path provided
	 * Note - not recursive
	 * @param path
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public abstract List<String> list(String path) throws IOException, InterruptedException;
	
	/**
	 * Move (without deleting) the file to the storage engine
	 * @param localFilePath
	 * @param storagePath
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public abstract void moveToStorage(String localFilePath, String storagePath) throws IOException, InterruptedException;
	
	
	/**
	 * Init the values for the rclone storage engine
	 * @param builder
	 */
	public void connect(Properties smssProp) {
		this.smssProp = smssProp;
	}
	
	/**
	 * 
	 * @param rcloneConfig
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void deleteRcloneConfig(String rcloneConfig) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		try {
			runRcloneProcess(rcloneConfig, "rclone", "config", "delete", rcloneConfig);
		} finally {
			new File(configPath).delete();
		}
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<String> runRcloneProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		commandList.add("--fast-list");
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);	
	}
	
	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<String> runRcloneTransferProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--transfers");
		commandList.add(TRANSFER_LIMIT);
		commandList.add("--config");
		commandList.add(configPath);
		commandList.add("--fast-list");
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);	
	}
	
	/**
	 * 
	 * @param rcloneConfig
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected List<String> runRcloneDeleteFileProcess(String rcloneConfig, String... command) throws IOException, InterruptedException {
		String configPath = getConfigPath(rcloneConfig);
		List<String> commandList = new ArrayList<>();
		commandList.addAll(Arrays.asList(command));
		commandList.add("--config");
		commandList.add(configPath);
		String[] newCommand = commandList.toArray(new String[] {});
		return runAnyProcess(newCommand);	
	}

	/**
	 * 
	 * @param rcloneConfig
	 * @return
	 */
	protected String getConfigPath(String rcloneConfig) {
		if( rcloneConfigFolder == null) {
			rcloneConfigFolder =  DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + FILE_SEPARATOR + "rcloneConfig";
			new File(rcloneConfig).mkdirs();
		}
		
		return rcloneConfigFolder + FILE_SEPARATOR + rcloneConfig + ".conf";
	}

	/**
	 * 
	 * @param command
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
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
	
	/**
	 * 
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	protected static List<String> streamOutput(InputStream stream) throws IOException {
		return stream(stream, false);
	}

	/**
	 * 
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	protected static List<String> streamError(InputStream stream) throws IOException {
		return stream(stream, true);
	}

	/**
	 * 
	 * @param stream
	 * @param error
	 * @return
	 * @throws IOException
	 */
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
