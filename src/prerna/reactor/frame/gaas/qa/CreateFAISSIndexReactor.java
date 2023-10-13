package prerna.reactor.frame.gaas.qa;

import java.io.File;
import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.py.PyTranslator;
import prerna.project.api.IProject;
import prerna.reactor.frame.gaas.GaasBaseReactor;
import prerna.reactor.frame.gaas.processors.CSVWriter;
import prerna.reactor.frame.gaas.processors.DocProcessor;
import prerna.reactor.frame.gaas.processors.PDFProcessor;
import prerna.reactor.frame.gaas.processors.PPTProcessor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class CreateFAISSIndexReactor extends GaasBaseReactor 
{

	public CreateFAISSIndexReactor()
	{
		// fileName - List of files
		// Name - the name for this config.json
		// baseFolder relative to the project

		this.keysToGet = new String [] {ReactorKeysEnum.FILE_NAME.getKey(), 
										ReactorKeysEnum.NAME.getKey(),
										ReactorKeysEnum.PROJECT.getKey(),
										ReactorKeysEnum.BASE_URL.getKey(),
										ReactorKeysEnum.DESCRIPTION.getKey(),
										ReactorKeysEnum.CONTENT_LENGTH.getKey(),
										ReactorKeysEnum.CONTENT_OVERLAP.getKey()
										};
		this.keyRequired = new int[] {1,1,1,1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		//organizeKeys();
		Logger logger = getLogger(this.getClass().getName());
		String projectId = getProjectId();
		
		Map configMap = new HashMap();
		
		List <String> processedList = new ArrayList<String>();
		List <String> unProcessedList = new ArrayList<String>();
		
		if(projectId == null)
		{
			return NounMetadata.getErrorNounMessage("Project is required for creating index ");
		}
		
		configMap.put("Project", projectId);
		
		String basePath = null;
		if(projectId.equalsIgnoreCase("temp"))
			basePath = "c:/temp";
		else
			basePath = AssetUtility.getProjectAssetFolder(projectId);
		
		basePath = basePath.replace("\\", "/");

		
		List <String> fileNames = this.store.getNoun(keysToGet[0]).getAllStrValues();
		String configName = this.store.getNoun(keysToGet[1]).get(0) +"";
		
		String baseURL = "";
		if(this.store.getNoun(keysToGet[3]) != null)
		{
			baseURL = this.store.getNoun(keysToGet[3]).get(0) + "";
		}
		if(this.store.getNoun(keysToGet[4]) != null)
		{
			String description = this.store.getNoun(keysToGet[4]).get(0) +"";
			configMap.put("Description", description);
		}
		if(baseURL == null)
			baseURL = "";
		
		String indexingFolder = basePath + baseURL;

		configMap.put("Name", configName);
		String csvFileName = indexingFolder + "/" + configName + ".csv";
		String configFile = indexingFolder + "/" + configName + ".json";
		String faiss_index = indexingFolder + "/" + configName + "_faiss.index";
		
		int contentLength = 512;
		if(this.store.getNoun(keysToGet[5]) != null)
			contentLength = Integer.parseInt(this.store.getNoun(keysToGet[5]).get(0) + "");
		
		int contentOverlap = 0;
		if(this.store.getNoun(keysToGet[6]) != null)
			contentOverlap = Integer.parseInt(this.store.getNoun(keysToGet[6]).get(0) + "");
		
		CSVWriter writer = new CSVWriter(csvFileName);
		writer.setTokenLength(contentLength);
		writer.overlapLength(contentOverlap);

		logger.info("Starting file conversions ");
		
		// pick up the files and convert them to CSV
		for(int fileIndex = 0;fileIndex < fileNames.size();fileIndex++)
		{
			String thisFile = fileNames.get(fileIndex);
			logger.info("Processing file : " + thisFile);

			String fileLocation = indexingFolder + "/" + thisFile;		
			// process this file
			Map fileMap = addFileProperties(fileLocation);
			if(fileMap != null)
			{
				String mimeType = fileMap.get("mime_type") + "";
				if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document"))
				{
					// document
					DocProcessor dp = new DocProcessor(fileLocation, writer);
					dp.process();
					configMap.put(thisFile, fileMap);
					processedList.add(thisFile);
				}
				else if(mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation"))
				{
					// powerpoint
					PPTProcessor pp = new PPTProcessor(fileLocation, writer);
					pp.process();
					configMap.put(thisFile, fileMap);
					processedList.add(thisFile);
				}
				else if(mimeType.equalsIgnoreCase("application/pdf"))
				{
					PDFProcessor pdf = new PDFProcessor(fileLocation, writer);
					pdf.process();
					configMap.put(thisFile, fileMap);
					processedList.add(thisFile);
				}
				else
				{
					String message = "We Currently do not support mime-type" + mimeType;
					configMap.put(thisFile, message);
					unProcessedList.add(thisFile);
				}

				logger.info("Completed Processing file : " + thisFile);
			}
		}
		
		configMap.put("Processed", processedList);
		configMap.put("Unprocessed", unProcessedList);
		
		logger.info("Creating index ");
		indexData(projectId, baseURL, configName, csvFileName, faiss_index);
		
		// write this to a json
		Gson gson = new Gson();
		String json = gson.toJson(configMap);
		
		try {
			FileUtils.writeStringToFile(new File(configFile), json);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//push to cloud
		
		if (ClusterUtil.IS_CLUSTER) {
	
		IProject project = Utility.getProject(projectId);
		String projectFolderPath = AssetUtility.getProjectBaseFolder(project.getProjectName(), projectId).replace("\\", "/");
		ClusterUtil.pushProjectFolder(project, projectFolderPath);

		}
		
		return new NounMetadata(configMap, PixelDataType.MAP);
	}
	
	// base folder is relative to the project
	
	private void indexData(String projectId, String baseFolder, String indexName, String inputFile, String indexFile)
	{
		// create the index on CSV
		// import gaas_simple_faiss as fa
		// from datasets import DataSet
		// ds = Dataset.from_csv(csvfileName, encoding="iso-8859-1")
		// f1 = fa.FAISSSearcher(ds=ds)
		// f1.custom_faiss_index()
		// f1.save_index(faiss_index)
		// delete everything
		projectId = projectId.replace("-", "_");
		baseFolder = baseFolder.replace("-", "_");
		baseFolder = baseFolder.replace("/", "_");
		baseFolder = baseFolder.replace(" ", "_");
		indexName=indexName.replace(" ","_");

		PyTranslator pyt = insight.getPyTranslator();
		String baseVarName = "a_" + projectId + "_" + baseFolder + "_" + indexName; 
		String [] commands = new String[] {
				"import gaas_simple_faiss as fa",
				"from datasets import Dataset",
				baseVarName + "_ds = Dataset.from_csv('" + inputFile + "', encoding='iso-8859-1')",
				baseVarName + "_faiss = fa.FAISSSearcher(ds=" + baseVarName + "_ds)", 
				baseVarName + "_faiss.custom_faiss_index()",
				baseVarName + "_faiss.save_index('" + indexFile + "')",
				//"del " + baseVarName + "_faiss",
				//"del " + baseVarName + "_ds",
				//"del " + baseVarName + " fa",
				//"del " + baseVarName + " Dataset"
		};
		
		pyt.runPyAndReturnOutput(commands);
	}
	
	private Map addFileProperties(String fileLocation)
	{
		Map fileMap = new HashMap();
		Path path = Paths.get(fileLocation);
		try
		{
			BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
			fileMap.put("create_time", attr.creationTime());
			fileMap.put("last_modified_time", attr.lastModifiedTime());
			fileMap.put("last_access_time", attr.lastAccessTime());
			fileMap.put("size", attr.size());
			String mimeType = null;
			if (SystemUtils.IS_OS_MAC) {
			     mimeType = URLConnection.guessContentTypeFromName(path.toFile().getName());

			} else {
				 mimeType = Files.probeContentType(path);
			}
	
			fileMap.put("mime_type", mimeType);
			
			fileMap.put("name", path.getFileName() +"");
			//fileMap.put("location", path); // need to remove the reference to project
		}catch(Exception ex)
		{
			return null;
		}
		System.err.println(fileMap);
		return fileMap;
	}
	
}
