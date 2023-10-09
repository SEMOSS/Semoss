package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.TCPPyTranslator;
import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.engine.impl.model.ModelEngineConstants;
import prerna.om.Insight;
import prerna.tcp.client.NativePySocketClient;
import prerna.util.Constants;
import prerna.util.Utility;

public class FaissDatabaseEngine extends AbstractVectorDatabaseEngine {

	private static final Logger classLogger = LogManager.getLogger(FaissDatabaseEngine.class);
	
	private static final String DIR_SEPARATOR = "/";
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static final String initScript = "import vector_database;${VECTOR_SEARCHER_NAME} = vector_database.FAISSDatabase(encoder_class = vector_database.get_encoder(encoder_type='${ENCODER_TYPE}', embedding_model='${ENCODER_NAME}', api_key = '${ENCODER_API_KEY}'))";
	
	protected String vectorDatabaseSearcher = null;
	
	File vectorDbFolder;
	File schemaFolder;
	
	List<String> indexClasses;
	
	// python server
	TCPPyTranslator pyt = null;
	NativePySocketClient socketClient = null;
	Process p = null;
	String port = null;
	String prefix = null;
	String pyDirectoryBasePath = null;
	File cacheFolder;
	//File indexFolder;
	//File documentsFolder;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		
		// highest directory (first layer inside vector db base folder)
		this.pyDirectoryBasePath = this.connectionURL + "py" + DIR_SEPARATOR;
		this.vectorDbFolder = new File(this.connectionURL);
		this.cacheFolder = new File(pyDirectoryBasePath.replace(FILE_SEPARATOR, DIR_SEPARATOR));

		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		this.schemaFolder = new File(this.connectionURL, "schema");
		if(!this.schemaFolder.exists()) {
			this.schemaFolder.mkdirs();
		}
		this.smssProp.put("WORKING_DIR", this.schemaFolder.getAbsolutePath());
		
		// third layer - All the separate tables,classes, or searchers that can be added to this db
		this.indexClasses = new ArrayList<>(Arrays.asList(this.schemaFolder.list()));
		
		//this.documentsFolder = new File(this.connectionURL + DIR_SEPARATOR + "documents");
		//this.indexFolder = new File(this.connectionURL + DIR_SEPARATOR + "indexed_files");
		
		this.vectorDatabaseSearcher = Utility.getRandomString(6);
		
		this.smssProp.put("VECTOR_SEARCHER_NAME", this.vectorDatabaseSearcher);	
	
		if (!this.smssProp.containsKey("ENCODER_API_KEY")) {
			this.smssProp.put("ENCODER_API_KEY", "");	
		}
		
		// vars for string substitution
		this.vars = new HashMap<>(this.smssProp);
	}
	
	public void startServer() {
		// spin the server
		// start the client
		// get the startup command and parameters - at some point we need a better way than the command
		
		// execute all the basic commands		
		
		// break the commands seperated by ;
		String [] commands = initScript.split(ModelEngineConstants.PY_COMMAND_SEPARATOR);
		
		// need to iterate through and potential spin up tables themselves
		if (this.indexClasses.size() > 0) {
	        ArrayList<String> modifiedCommands = new ArrayList<>(Arrays.asList(commands));
			for (String table : this.indexClasses) {
				File fileToCheck = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + table, "dataset.pkl");
				modifiedCommands.add("${VECTOR_SEARCHER_NAME}.create_searcher(searcher_name = '"+table+"', base_path = '"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"')");
				if (fileToCheck.exists()) {
			        modifiedCommands.add("${VECTOR_SEARCHER_NAME}.searchers['"+table+"'].load_dataset('"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"' + 'dataset.pkl')");
			        modifiedCommands.add("${VECTOR_SEARCHER_NAME}.searchers['"+table+"'].load_encoded_vectors('"+fileToCheck.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"' + 'vectors.pkl')");
		        }
			}
            commands = modifiedCommands.stream().toArray(String[]::new);
		}

		
		
		// replace the Vars
		for(int commandIndex = 0; commandIndex < commands.length;commandIndex++) {
			commands[commandIndex] = fillVars(commands[commandIndex]);
		}
		
		port = Utility.findOpenPort();
		
		String timeout = "30";
		if(smssProp.containsKey(Constants.IDLE_TIMEOUT))
			timeout = smssProp.getProperty(Constants.IDLE_TIMEOUT);

		if(!this.cacheFolder.exists()) {
			this.cacheFolder.mkdirs();
		}
		
		Object [] outputs = Utility.startTCPServerNativePy(this.cacheFolder.getAbsolutePath(), port, timeout);
		this.p = (Process) outputs[0];
		this.prefix = (String) outputs[1];
		
		socketClient = new NativePySocketClient();
		socketClient.connect("127.0.0.1", Integer.parseInt(port), false);
		
		// connect the client
		connectClient();
		
		// create the py translator
		pyt = new TCPPyTranslator();
		pyt.setClient(socketClient);
	
		pyt.runEmptyPy(commands);
	}
	
	@SuppressWarnings("unchecked")
	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}
	
	public boolean connectClient() {
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
	
	@Override
	public void addDocumet(List<String> filePaths, Map <String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		File parentDirectory = (File) parameters.get("temporaryFileDirectory");
		
		// first we need to extract the text from the document
		// TODO change this to json so we never have an encoding issue
		checkSocketStatus();
		
		List<String> filesToIndex;
		File tableDirectory = new File(this.schemaFolder, indexClass);
		File documentDir = new File(tableDirectory, "documents");
		if(!documentDir.exists()) {
			documentDir.mkdirs();
		}
		boolean filesAppoved = FaissDatabaseUtils.verifyFileTypes(filePaths, new ArrayList<>(Arrays.asList(documentDir.list())));
		if (!filesAppoved) {
			// delete them all
			try {
				FileUtils.forceDelete(parentDirectory);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete the temporary file directory");
			}
			throw new IllegalArgumentException("Currently unable to mix csv with non-csv file types.");
		}
		
		File tableIndexFolder = new File(this.schemaFolder + DIR_SEPARATOR + indexClass, "indexed_files");
		if (!tableIndexFolder.exists()) {
			tableIndexFolder.mkdirs();
		}
		if (!this.indexClasses.contains(indexClass)) {
			this.indexClasses.add(indexClass);
			this.pyt.runScript(this.vectorDatabaseSearcher + ".create_searcher(searcher_name = '"+indexClass+"', base_path = '"+tableIndexFolder.getParent().replace(FILE_SEPARATOR, DIR_SEPARATOR) + DIR_SEPARATOR +"')");
		}
		
		String columnsToIndex = "";
		List <String> extractedFiled = new ArrayList<String>();
		for (String fileName : filePaths) {
			// move the documents into documents folder
			File fileInTempFolder = new File(fileName);
			
			// TODO probably need to handle zips
			if (!fileInTempFolder.isFile()) {
				continue;
			}
			
			File destinationFile = new File(documentDir, fileInTempFolder.getName());
			
			// Check if the destination file exists, and if so, delete it
			try {
				if (destinationFile.exists()) {
					FileUtils.forceDelete(destinationFile);
	            }
				FileUtils.moveFileToDirectory(fileInTempFolder, documentDir, true);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove previously created file for " + destinationFile.getName() + " or move it to the document directory");
			}
			
			String documentName = destinationFile.getName().split("\\.")[0];
			File extractedFile = new File(tableIndexFolder.getAbsolutePath() + DIR_SEPARATOR + documentName + ".csv");
			try {
				if (extractedFile.exists()) {
					FileUtils.forceDelete(extractedFile);
				}
				if (!destinationFile.getName().toLowerCase().endsWith(".csv")) {
					FaissDatabaseUtils.convertFilesToCSV(extractedFile.getAbsolutePath(), this.contentLength, this.contentOverlap, destinationFile);
					columnsToIndex = "'Content'";
				} else {
					// copy csv over but make sure its only csvs
					FileUtils.copyFileToDirectory(destinationFile, tableIndexFolder);
					columnsToIndex = "";
					// TODO get columns to index for csvs
				}
				extractedFiled.add(extractedFile.getAbsolutePath().replace(FILE_SEPARATOR, DIR_SEPARATOR));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to remove olf or create new text extraction file for " + documentName);
			}
		}
		
		// create dataset
		String script = vectorDatabaseSearcher +  ".searchers['"+indexClass+"'].addDocumet(documentFileLocation = ['" + String.join("','", extractedFiled) + "'],columns_to_index = ["+columnsToIndex+"])";
		classLogger.info("Running >>>" + script);
		this.pyt.runScript(script);
		try {
			FileUtils.forceDelete(parentDirectory);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to delete the temporary file directory");
		}
	}

	@Override
	public void removeDocument(List<String> filePaths, Map <String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		if (!this.indexClasses.contains(indexClass)) {
			throw new IllegalArgumentException("Unable to remove documents from a directory that does not exist");
		}
		
		checkSocketStatus();
		
		String indexedFilesPath = this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "indexed_files";
		Path indexDirectory = Paths.get(indexedFilesPath);
		for (String document : filePaths) {
			String documentName = document.split("\\.")[0];
	        String[] fileNamesToDelete = {documentName + "_dataset.pkl", documentName + "_vectors.pkl", documentName + ".csv"};

	        // Create a filter for the file names
	        DirectoryStream.Filter<Path> fileNameFilters = entry -> {
	            String fileName = entry.getFileName().toString();
	            for (String fileNameToDelete : fileNamesToDelete) {
	                if (fileName.equals(fileNameToDelete)) {
	                    return true;
	                }
	            }
	            return false;
	        };
	        
	        DirectoryStream<Path> stream;
			try {
				stream = Files.newDirectoryStream(indexDirectory, fileNameFilters);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable determine files in " + indexDirectory.getFileName());
			}
	        for (Path entry : stream) {
                // Delete each file that matches the specified file name
                try {
					Files.delete(entry);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException("Unable to remove file: " + entry.getFileName());
				}
                classLogger.info("Deleted: " + entry.toString());
            }
	        try {
				FileUtils.forceDelete(new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents", document));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete " + document + "from documents directory");
			}
		}
		
		// this would me the table is now empty, we should delete it
		File indexedFolder = new File(indexedFilesPath);
		if (indexedFolder.list().length == 0) {
			try {
				FileUtils.forceDelete(new File(indexedFolder.getParent()));
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to delete remove the index class folder");
			}
			this.pyt.runScript(this.vectorDatabaseSearcher + ".delete_searcher(searcher_name = '"+indexClass+"')");
			this.indexClasses.remove(indexClass);
		} else {
			String script = this.vectorDatabaseSearcher + ".searchers['"+indexClass+"'].createMasterFiles(path_to_files = '" + indexedFilesPath.replace(FILE_SEPARATOR, DIR_SEPARATOR) + "')";
			this.pyt.runScript(script);
		}
	}

	@Override
	public Object nearestNeighbor(String question, int limit, Map <String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		// TODO we will need the insight when runnings encode call through IModelEngine
		//Insight insight = (Insight) parameters.get("insight");
		//if (insight == null) {
		//	throw new IllegalArgumentException("Insight must be provided to run Model Engine Encoder");
		//}
		
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
		
		StringBuilder callMaker = new StringBuilder();
		
		// make the python method
		callMaker.append(this.vectorDatabaseSearcher)
				 .append(".searchers['")
				 .append(indexClass)
				 .append("']")
				 .append(".get_result_faiss(");
		
		// make the question arg
		callMaker.append("question=\"\"\"")
				 .append(question.replace("\"", "\\\""))
				 .append("\"\"\"");
		
		// make the limit, i.e. the number of responses we want
		callMaker.append(",")
				 .append("results = ")
				 .append(limit);
		
		// TODO implements logic to sort results and apply threshold
		
		// close the method
 		callMaker.append(")");
 		
		Object output = pyt.runScript(callMaker.toString());
		return output;
	}
	
	@Override
	public String[] listDocuments(Map<String, Object> parameters) {
		String indexClass = this.defaultIndexClass;
		if (parameters.containsKey("indexClass")) {
			indexClass = (String) parameters.get("indexClass");
		}
		
		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + "documents");
		return documentsDir.list();
	}

	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return VectorDatabaseTypeEnum.FAISS;
	}
	
	private void checkSocketStatus() {
		if(this.socketClient == null || !this.socketClient.isConnected()) {
			this.startServer();
		}
	}
	
	@Override
	public void close() {
		if (this.socketClient.isConnected() && this.p.isAlive()) {
			this.socketClient.stopPyServe(this.pyDirectoryBasePath);
			this.socketClient.disconnect();
			this.socketClient.setConnected(false);
			this.pyDirectoryBasePath = null;
			this.p.destroy();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Properties tempSmss = new Properties();
		tempSmss.put("CONNECTION_URL", "Semoss_Dev/vector/");
		tempSmss.put("VECTOR_TYPE", "FAISS");
		tempSmss.put("INDEX_CLASSES", "default");
		tempSmss.put("ENCODER_TYPE", "huggingface");
		tempSmss.put("ENCODER_NAME", "sentence-transformers/paraphrase-mpnet-base-v2");
		
		FaissDatabaseEngine engine = new FaissDatabaseEngine();
		engine.open(tempSmss);
		
		engine.close();
	}
}
