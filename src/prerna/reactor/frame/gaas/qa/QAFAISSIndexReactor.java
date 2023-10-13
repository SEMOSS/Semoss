package prerna.reactor.frame.gaas.qa;

import java.io.File;

import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.project.api.IProject;
import prerna.reactor.frame.gaas.GaasBaseReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QAFAISSIndexReactor extends GaasBaseReactor 
{

	public QAFAISSIndexReactor()
	{
		// fileName - List of files
		// Name - the name for this config.json
		// baseFolder relative to the project

		this.keysToGet = new String [] {
										ReactorKeysEnum.NAME.getKey(),
										ReactorKeysEnum.PROJECT.getKey(),
										ReactorKeysEnum.BASE_URL.getKey(),
										ReactorKeysEnum.COMMAND.getKey(),
										ReactorKeysEnum.ROW_COUNT.getKey(),
										"useLLM"
										};
		this.keyRequired = new int[] {1,1,1};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		//organizeKeys();
		Logger logger = getLogger(this.getClass().getName());
		String projectId = getProjectId();
		
		if(projectId == null)
			return NounMetadata.getErrorNounMessage("Project is required for creating index ");

		String basePath = null;
		if(projectId.equalsIgnoreCase("temp"))
			basePath = "c:/temp";
		else
			basePath = AssetUtility.getProjectAssetFolder(projectId);
		
		basePath = basePath.replace("\\", "/");
		
		String configName = this.store.getNoun(keysToGet[0]).get(0) +"";
		
		String baseURL = "";
		if(this.store.getNoun(keysToGet[2]) != null)
		{
			baseURL = "/" + this.store.getNoun(keysToGet[2]).get(0);
		}
		if(baseURL == null)
			baseURL = "";

		
		String command = null;
		if(this.store.getNoun(keysToGet[3]) != null)
		{
			command = this.store.getNoun(keysToGet[3]).get(0) + "";
		}
		
		if(command == null)
			return NounMetadata.getErrorNounMessage("No search string provided ");			
		
		int count = 2;
		if(this.store.getNoun(keysToGet[4]) != null)
		{
			count = Integer.parseInt(this.store.getNoun(keysToGet[4]).get(0) + "");
		}
		
		Boolean useLLM = false;
		if(this.store.getNoun(keysToGet[5]) != null)
		{
			useLLM = (boolean) (this.store.getNoun(keysToGet[5]).get(0));
		}
		
		String indexingFolder = basePath + baseURL;

		String csvFileName = indexingFolder + "/" + configName + ".csv";
		String configFile = indexingFolder + "/" + configName + ".json";
		String faiss_index = indexingFolder + "/" + configName + "_faiss.index";
		
		File faiss_file = new File(faiss_index);
		if(!faiss_file.exists())
			return NounMetadata.getErrorNounMessage("No index file present, please index before trying");

		File csv_file = new File(csvFileName);
		if(!csv_file.exists())
			return NounMetadata.getErrorNounMessage("No data file present, please check again ");

		String output = searchData(projectId, baseURL, csvFileName, faiss_index, configName, command, count,useLLM);
		
		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}
	
	// base folder is relative to the project
	
	private String searchData(String projectId, String baseFolder, String inputFile, String indexFile, String indexName, String command, int count, Boolean useLLM)
	{
		// import gaas_simple_faiss as fa
		// import faiss
		// from datasets import Dataset
		// f1 = fa.FAISSSearcher()
		// ds = Dataset.from_csv(csvFileName, encoding='iso-8859-1')
		// index = faiss.read_index(faiss_index)
		// f1.get_result_faiss(ds=ds, index=index, results=count)
		
		IProject project = Utility.getProject(projectId);
		projectId = projectId.replace("-", "_");
		baseFolder = baseFolder.replace("-", "_");
		baseFolder = baseFolder.replace("/", "_");
		baseFolder = baseFolder.replace(" ", "_");
		indexName=indexName.replace(" ","_");
		
		PyTranslator pyt = project.getProjectPyTranslator();

		String baseVarName = "a_" + projectId + "_" + baseFolder + "_" + indexName; 
		String [] commands = new String[] {
				"import gaas_simple_faiss as fa", //0
				"from datasets import Dataset", //1
				"import faiss", //2
				baseVarName + "_ds = Dataset.from_csv('" + inputFile + "', encoding='iso-8859-1')",//3
				"a_" + projectId+ "_faiss = fa.FAISSSearcher()", //4
				baseVarName + "_index = faiss.read_index('" + indexFile + "')"
		};
		
		String [] searchCommands;
		if(useLLM) {
			
			String endpoint = DIHelper.getInstance().getProperty(Constants.GUANACO_ENDPOINT);
			if(endpoint == null || endpoint.trim().isEmpty()) {
				throw new IllegalArgumentException("Must define endpoint to run custom models");
			} 

			 searchCommands = new String [] {				
					 "print(a_" + projectId + "_faiss.qaLLM('" + command + "', "
					  + "results=" + count + ", "
					  + "ds=" + baseVarName + "_ds, "
					  + "index=" + baseVarName + "_index,"
					  +"endpoint='" + endpoint + "'))"
			};
			
		} else {
			searchCommands = new String [] {				
					 "print(a_" + projectId + "_faiss.qa('" + command + "', "
					  + "results=" + count + ", "
					  + "ds=" + baseVarName + "_ds, "
					  + "index=" + baseVarName + "_index))" //6
			};
		}
		// check to see if the faiss index exists if so no load it
		boolean hasIndex = (Boolean)pyt.runScript("'" + baseVarName + "_index' in locals()");
		if(hasIndex)
			commands[5] = "";

		boolean hasFaiss = (Boolean)pyt.runScript("'a_" + projectId + "_faiss' in locals()");
		if(hasFaiss)
			commands[4] = "";

		// check to see if dataset exists if so not load it
		boolean hasDS = (Boolean)pyt.runScript("'" + baseVarName + "_ds' in locals()");
		if(hasDS)
			commands[3] = "";

		if(!hasIndex || !hasFaiss || !hasDS)
			pyt.runPyAndReturnOutput(commands);
		
		String output = pyt.runPyAndReturnOutput(searchCommands);
		
		return output;
	}
}
