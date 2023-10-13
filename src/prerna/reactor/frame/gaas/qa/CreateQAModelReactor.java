package prerna.reactor.frame.gaas.qa;

import prerna.ds.py.PyTranslator;
import prerna.reactor.frame.gaas.GaasBaseReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class CreateQAModelReactor extends GaasBaseReactor {

	// creates a qa model
	final String modelType = "gaas";
	final String modelSubType = "qa";
		
	// the model string to send is "siamese / haystack /  somehting else" Right now only siamese is implemented	

	public CreateQAModelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.MODEL.getKey(), ReactorKeysEnum.COLUMN.getKey()};
		this.keyRequired = new int[] {1,1,0};
	}

	
	
	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		// get the folder name
		// see if the processed folder is already there
		// if so pass the processed folder with the model to invoke
		
		String folderName = keyValue.get(keysToGet[0]);
		String modelName = keyValue.get(keysToGet[1]);
		
		String columnName = "Content";
		if(keyValue.containsKey(keysToGet[2]))
			columnName = keyValue.get(keysToGet[2]);

		
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
		
		// if all goes well we are set
		// we will call the model by the folder name
		String modelVariable = projectId;
		modelVariable = Utility.cleanString(modelVariable, true);
		modelVariable = modelVariable.replace("-", "_");
		modelVariable = semossModelName + "_" + modelVariable;
		
		pt.runScript(modelVariable + " = " +semossModelName + ".create_model(folder_name='" + folderName + "', content_column= '" + columnName + "')");
		boolean hasModel = (Boolean)pt.runScript("'" + modelVariable + "' in locals()");
		
		return new NounMetadata("Model Created : " + hasModel, PixelDataType.CONST_STRING);
	}
}
