package prerna.reactor.frame.gaas.qa;

import prerna.ds.py.PyTranslator;
import prerna.reactor.frame.gaas.GaasBaseReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class ClearModelsReactor extends GaasBaseReactor {

	// creates a qa model
	final String modelType = "gaas";
	final String modelSubType = "qa";
	
	// the model string to send is "siamese / haystack /  somehting else" Right now only siamese is implemented	
	public ClearModelsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.MODEL.getKey()};
		this.keyRequired = new int[] {1, 1};
	}

	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
	
		// get the folder name
		// see if the processed folder is already there
		// if so pass the processed folder with the model to invoke		
		String folderName = keyValue.get(keysToGet[0]);
		String modelName = keyValue.get(keysToGet[1]);
		
		String projectId = getProjectId();
		String basePath = AssetUtility.getProjectAssetFolder(projectId);

		folderName = basePath + "/" + folderName;
		folderName = folderName.replace("\\", "/");
		

		String semossModelName = modelType + "_" + modelSubType + "_" + modelName;

		String modelVariable = projectId;
		modelVariable = Utility.cleanString(modelVariable, true);
		modelVariable = modelVariable.replace("-", "_");
		modelVariable = semossModelName + "_" + modelVariable;

		PyTranslator pt = this.insight.getPyTranslator();
		
		pt.runScript("import " + semossModelName);
		pt.runScript("del " + modelVariable);
		
		//  gaas.search_siamese(folder_name="c:/users/pkapaleeswaran/workspacej3/datasets/text/far"
		//, model=model, query="what is subcontract", 
		// separator="FAR::::")	
		pt.runScript(semossModelName + ".delete_model(folder_name='" + folderName + "')");
		pt.runScript(semossModelName + ".delete_processed(folder_name='" + folderName + "')");
				
		return new NounMetadata(keyValue.get(keysToGet[0]) + " Clear for new processing", PixelDataType.CONST_STRING);
	}

}
