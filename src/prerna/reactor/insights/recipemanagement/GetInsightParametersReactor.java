package prerna.reactor.insights.recipemanagement;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.parsers.ParamStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetInsightParametersReactor extends AbstractInsightParameterReactor {

	private static final Logger logger = LogManager.getLogger(GetInsightParametersReactor.class);

	@Override
	public NounMetadata execute() {
		VarStore varStore = this.insight.getVarStore();
		// loop through all the parameters
		// and return the parameter list
		List<String> parameterKeys = varStore.getInsightParameterKeys();
		List<ParamStruct> paramList = new Vector<>();
		for(String paramName : parameterKeys) {
			NounMetadata paramNoun = varStore.get(paramName);
			if(paramNoun != null) {
				paramList.add((ParamStruct) paramNoun.getValue());
			} else {
				logger.info("Unable to find parameter name = " + paramName);
			}
		}
		
		NounMetadata pStructNoun = new NounMetadata(paramList, PixelDataType.PARAM_STRUCT);
		return pStructNoun;
	}

}
