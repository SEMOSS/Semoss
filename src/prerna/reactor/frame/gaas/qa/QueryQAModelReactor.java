package prerna.reactor.frame.gaas.qa;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.ds.py.PyTranslator;
import prerna.reactor.frame.gaas.GaasBaseReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class QueryQAModelReactor extends GaasBaseReactor {

	// creates a qa model
	final String modelType = "gaas";
	final String modelSubType = "qa";
	
	// the model string to send is "siamese / haystack /  somehting else" Right now only siamese is implemented	
	public QueryQAModelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), 
				ReactorKeysEnum.MODEL.getKey(), 
				ReactorKeysEnum.THRESHOLD.getKey(), 
				ReactorKeysEnum.COMMAND.getKey(), 
				ReactorKeysEnum.ROW_COUNT.getKey(), 
				ReactorKeysEnum.SOURCE.getKey(),
				ReactorKeysEnum.COLUMN.getKey()
				};
		this.keyRequired = new int[] {1,
									  1, 
									  0,
									  1,
									  0,
									  0,
									  0};
	}

	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String query = keyValue.get(keysToGet[3]);
		int numRows = 3;
		String source = "False";
		
		if(keyValue.containsKey(keysToGet[4])) // row count
			numRows = Integer.parseInt(keyValue.get(keysToGet[4]));

		if(keyValue.containsKey(keysToGet[5])) // include source
			source = keyValue.get(keysToGet[5]).equalsIgnoreCase("True") ? "True" : "False";

		
		double threshold = 0.2;
		if(keyValue.containsKey(keysToGet[2]))
			threshold = Double.parseDouble(keyValue.get(keysToGet[2]))/100;
		// get the folder name
		// see if the processed folder is already there
		// if so pass the processed folder with the model to invoke
		
		String folderName = keyValue.get(keysToGet[0]);
		String modelName = keyValue.get(keysToGet[1]);
		
		
		String projectId = getProjectId();
		String basePath = AssetUtility.getProjectAssetFolder(projectId);

		folderName = basePath + "/" + folderName;
		folderName = folderName.replace("\\", "/");
		
		System.err.println("Folder.. " + folderName);
			

		String semossModelName = modelType + "_" + modelSubType + "_" + modelName;

		PyTranslator pt = this.insight.getPyTranslator();
		pt.runScript("import " + semossModelName);
		//  gaas.search_siamese(folder_name="c:/users/pkapaleeswaran/workspacej3/datasets/text/far"
		//, model=model, query="what is subcontract", 
		// separator="FAR::::")
		
		String modelVariable = projectId;
		modelVariable = Utility.cleanString(modelVariable, true);
		modelVariable = modelVariable.replace("-", "_");
		modelVariable = semossModelName + "_" + modelVariable;

		boolean hasModel = (Boolean)pt.runScript("'" + modelVariable + "' in locals()");
		if(!hasModel)
		{
			pt.runScript(modelVariable + " = " + semossModelName + ".hydrate_model(folder_name='" + folderName + "')");
		}
		
		String masterDocument = modelVariable + "_master_document"; // also load it once
		boolean hasDocument = (Boolean)pt.runScript("'" + masterDocument + "' in locals()");
		
		if(!hasDocument)
		{
			pt.runScript(masterDocument + " = " + semossModelName + ".get_master_document(folder_name='" + folderName + "')");
		}
		
			// the model to call it
		List results	= (List)pt.runScript(semossModelName + ".search("
												+ "folder_name='" + folderName + "', "
												+ "model=" + modelVariable +", "
												+ "threshold=" + threshold + ", "
												+ "query='" + query + "'" + ","
												+ "result_count = " + numRows + ", "
												+ "source=" + source + ","
												+ "master_document=" + masterDocument
												+ ")", this.insight
												);
		
		//pt.runScript(modelVariable + " = " +semossModelName + ".create_model(folder_name='" + folderName + "')");
		
		Map outputMap = new HashMap();
		outputMap.put("query", query);
		outputMap.put("data", results);
		
		return new NounMetadata(outputMap, PixelDataType.MAP);
	}

}
