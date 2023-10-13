package prerna.reactor.legacy.playsheets;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.om.SEMOSSParam;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;

public class LegacyInsightDatabaseUtility {

	private static final Logger classLogger = LogManager.getLogger(LegacyInsightDatabaseUtility.class);

	// get param information when i have the question id
	private static final String QUESTION_ID_FK_PARAM_KEY = "@QUESTION_ID_FK_VALUES@";
	private static final String GET_ALL_PARAMS_FOR_QUESTION_ID = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID, PARAMETER_ID FROM PARAMETER_ID WHERE QUESTION_ID_FK = " + QUESTION_ID_FK_PARAM_KEY;
	
	// get param information when i have the param id
	private static final String PARAMETER_ID_PARAM_KEY = "@PARAMETER_ID";
	private static final String GET_INFO_FOR_PARAMS = "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID FROM PARAMETER_ID WHERE PARAMETER_ID IN (" + PARAMETER_ID_PARAM_KEY + ")";
	
	private LegacyInsightDatabaseUtility() {
		
	}
	
	/**
	 * Get the parameters for the insight id
	 * @param insightId
	 * @return
	 */
	public static List<SEMOSSParam> getParamsFromInsightId(IDatabaseEngine insightRdbms, String insightId) {
		String query = GET_ALL_PARAMS_FOR_QUESTION_ID.replace(QUESTION_ID_FK_PARAM_KEY, insightId);
		List<SEMOSSParam> retParam = new Vector<SEMOSSParam>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(insightRdbms, query);
			while(wrapper.hasNext()) {
				// get a bunch of options
				IHeadersDataRow ss = wrapper.next();
				Object[] dataRow = ss.getValues();
				String label = dataRow[0] + "";
				SEMOSSParam param = new SEMOSSParam();
				param.setName(label);
				Object type = dataRow[1];
				if(type != null && !type.toString().isEmpty()) {
					param.setType(type.toString());
				}
				Object options = dataRow[2];
				if(options != null && !options.toString().isEmpty()) {
					param.setOptions(options.toString());
				}
				Object paramQuery = dataRow[3];
				if(paramQuery != null && !paramQuery.toString().isEmpty()) {
					param.setQuery(paramQuery.toString());
				}
				Object paramDependency = dataRow[4];
				if(paramDependency != null && !paramDependency.toString().isEmpty()) {
					String[] vars = paramDependency.toString().split(";");
					for(String var : vars){
						param.addDependVar(var);
					}
					param.setQuery(paramQuery.toString());
				}
				Object isDbQuery = dataRow[5];
				if(isDbQuery != null) {
					param.setDbQuery((boolean) isDbQuery);
				}
				Object isMultiSelect = dataRow[6];
				if(isDbQuery != null) {
					param.setMultiSelect((boolean) isMultiSelect);
				}
				Object componentFilter = dataRow[7];
				if(componentFilter != null && !componentFilter.toString().isEmpty()) {
					param.setComponentFilterId(componentFilter.toString());
				}
				Object paramId = dataRow[8];
				if(paramId != null && !paramId.toString().isEmpty()) {
					param.setParamID(paramId.toString());
				}
				// add to the set
				retParam.add(param);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return retParam;
	}
	
	
	public static Vector<SEMOSSParam> getParamsFromParamIds(IDatabaseEngine insightRdbms, String... paramIds) {
		String pIdString = "";
		int numIDs = paramIds.length;
		for(int i = 0; i < numIDs; i++) {
			String id = paramIds[i];
			pIdString = pIdString + "'" + id + "'";
			if(i != numIDs - 1) {
				pIdString = pIdString + ", ";
			}
		}
		String query = GET_INFO_FOR_PARAMS.replace(PARAMETER_ID_PARAM_KEY, pIdString);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRdbms, query);
		String[] names = wrap.getVariables();

		Vector<SEMOSSParam> retParams = new Vector<SEMOSSParam>();
		while(wrap.hasNext()) {
			ISelectStatement ss = wrap.next();
			String label = ss.getVar(names[0]) + "";
			SEMOSSParam param = new SEMOSSParam();
			param.setName(label);
			if(ss.getVar(names[1]) != null)
				param.setType(ss.getVar(names[1]) +"");
			if(ss.getVar(names[2]) != null)
				param.setOptions(ss.getVar(names[2]) + "");
			if(ss.getVar(names[3]) != null)
				param.setQuery(ss.getVar(names[3]) + "");
			if(ss.getRawVar(names[4]) != null)
				param.addDependVar(ss.getRawVar(names[4]) +"");
			if(ss.getVar(names[5]) != null && !ss.getVar(names[5]).toString().isEmpty())
				param.setDbQuery((boolean) ss.getVar(names[5]));
			if(!ss.getVar(names[6]).toString().isEmpty())
				param.setMultiSelect((boolean) ss.getVar(names[6]));
			if(!ss.getVar(names[7]).toString().isEmpty())
				param.setComponentFilterId(ss.getVar(names[7]) + "");
			if(ss.getVar(names[0]) != null)
				param.setParamID(ss.getVar(names[0]) +"");
			
			retParams.addElement(param);
		}		
		
		return retParams;
	}

	
}
