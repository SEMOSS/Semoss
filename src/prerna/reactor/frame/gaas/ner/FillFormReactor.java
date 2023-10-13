package prerna.reactor.frame.gaas.ner;

import prerna.ds.py.PyTranslator;
import prerna.reactor.frame.gaas.GaasBaseReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class FillFormReactor extends GaasBaseReactor {

	// creates a qa model
	final String modelType = "gaas";
	final String modelSubType = "ner";
	
	// the model string to send is "siamese / haystack /  somehting else" Right now only siamese is implemented	
	public FillFormReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.FIELDS.getKey(), ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1, 1, 0};
	}

	
	
	@Override
	public NounMetadata execute() {
		
		//organizeKeys();
		
		String projectId = getProjectId();

		if(projectId == null) // project is not important
			projectId = "UNKNOWN";
		
		// get the folder name
		// see if the processed folder is already there
		// if so pass the processed folder with the model to invoke		
		String inputContext = this.store.getNoun(this.keysToGet[0]).get(0).toString();
		GenRowStruct colGrs = this.store.getNoun(this.keysToGet[1]);
		
		StringBuffer fields = new StringBuffer("[");
		if (colGrs != null && !colGrs.isEmpty()) 
		{
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) 
			{
				if(selectIndex > 0)
					fields.append(",");
				String column = colGrs.get(selectIndex) + "";
				fields.append("'").append(column).append("'");
			}
			fields.append("]");
		}
		else
			NounMetadata.getWarningNounMessage("No fields to query");
		
		PyTranslator pt = this.insight.getPyTranslator();
		
		String semossModelName = "gaas_ner_form";
		
		// import
		pt.runScript("import " + semossModelName);
		
		
		String modelVariable = projectId;
		modelVariable = Utility.cleanString(modelVariable, true);
		modelVariable = modelVariable.replace("-", "_");
		modelVariable = semossModelName + "_" + modelVariable;

		// create the pipe
		pt.runScript(modelVariable + " = " + semossModelName + ".init()");
		
		// get the responses now
		Object output = pt.runScript(semossModelName + ".search_form_fields(input_data='" + inputContext + "', form_fields=" + fields + ", pipe=" + modelVariable + ")");
						
		System.err.println("Map is " + output);
		return new NounMetadata(output, PixelDataType.MAP);
	}

}
