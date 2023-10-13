package prerna.reactor.legacy.playsheets;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.om.SEMOSSParam;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetPlaysheetParamsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetPlaysheetParamsReactor.class);

	private static final String PARAMETER_ID_PARAM_KEY = "@PARAMETER_ID";
	// "SELECT DISTINCT PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_OPTIONS, PARAMETER_QUERY, PARAMETER_DEPENDENCY, PARAMETER_IS_DB_QUERY, PARAMETER_MULTI_SELECT, PARAMETER_COMPONENT_FILTER_ID FROM PARAMETER_ID WHERE PARAMETER_ID = '" + PARAMETER_ID_PARAM_KEY + "'";
	private static final String GET_INFO_FOR_PARAM = "SELECT DISTINCT "
					+ "PARAMETER_LABEL, "
					+ "PARAMETER_TYPE, "
					+ "PARAMETER_OPTIONS, "
					+ "PARAMETER_QUERY, "
					+ "PARAMETER_DEPENDENCY, "
					+ "PARAMETER_IS_DB_QUERY, "
					+ "PARAMETER_MULTI_SELECT, "
					+ "PARAMETER_COMPONENT_FILTER_ID "
					+ "FROM PARAMETER_ID "
					+ "WHERE PARAMETER_ID = '" + PARAMETER_ID_PARAM_KEY + "'";

	public GetPlaysheetParamsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		// TODO: ACCOUNTING FOR LEGACY PLAYSHEETS
		if(projectId == null) {
			projectId = this.store.getNoun("app").get(0) + "";
		}
		String insightId = this.keyValue.get(this.keysToGet[1]);
		IProject project = Utility.getProject(projectId);
		Insight in = project.getInsight(insightId).get(0);
		Map<String, Object> outputHash = new Hashtable<String, Object>();
		outputHash.put("result", in.getRdbmsId());
		if (in.isOldInsight()) {
			// get the parameters
			List<SEMOSSParam> paramVector = ((OldInsight) in).getInsightParameters();
			// try to flush them out as long as they dont have a depends
			Map<String, Object> optionsHash = new Hashtable<String, Object>();
			Map<String, Object> paramsHash = new Hashtable<String, Object>();
			for (int paramIndex = 0; paramIndex < paramVector.size(); paramIndex++) {
				SEMOSSParam param = paramVector.get(paramIndex);
				if (param.isDepends().equalsIgnoreCase("false")) {
					List<Object> vals = getParamOptions(project, param.getParamID());
					Set<Object> uniqueVals = new HashSet<Object>(vals);
					optionsHash.put(param.getName(), uniqueVals);
				} else {
					optionsHash.put(param.getName(), "");
				}
				paramsHash.put(param.getName(), param);
			}
			outputHash.put("options", optionsHash);
			outputHash.put("params", paramsHash);
		}
		return new NounMetadata(outputHash, PixelDataType.MAP, PixelOperationType.PLAYSHEET_PARAMS);
	}

	public List<Object> getParamOptions(IProject project, String parameterID) {
		IDatabaseEngine insightRDBMS = project.getInsightDatabase();
		// TODO: ACCOUNTING FOR LEGACY PLAYSHEETS
		IDatabaseEngine mainEngine = Utility.getDatabase(project.getProjectId());
		
		String query = GET_INFO_FOR_PARAM.replace(PARAMETER_ID_PARAM_KEY, parameterID);
		ISelectWrapper wrap = WrapperManager.getInstance().getSWrapper(insightRDBMS, query);
		String[] names = wrap.getVariables();

		Vector<SEMOSSParam> retParam = new Vector<SEMOSSParam>();
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
			retParam.addElement(param);
		}

		List<Object> uris = new Vector<Object>();
		if(!retParam.isEmpty()){
			SEMOSSParam ourParam = retParam.get(0); // there should only be one as we are getting the param from a specific param URI
			//if the param has options defined, we are all set
			//grab the options and we are good to go
			Vector<String> options = ourParam.getOptions();
			if (options != null && !options.isEmpty()) {
				uris.addAll(options);
			}
			else{
				// if options are not defined, need to get uris either from custom sparql or type
				// need to use custom query if it has been specified in the xml
				// otherwise use generic fill query
				String paramQuery = ourParam.getQuery();
				String type = ourParam.getType();
				boolean isDbQuery = ourParam.isDbQuery();
				// RDBMS right now does type:type... need to get just the second type. This will be fixed once insights don't store generic query
				// TODO: fix this logic. need to decide how to store param type for rdbms
				if(paramQuery != null && !paramQuery.isEmpty()) {
					//TODO: rdbms has type as null... this is confusing given the other comments here....
					if(type != null && !type.isEmpty()) {
						if (mainEngine.getDatabaseType() == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
							if (type.contains(":")) {
								String[] typeArray = type.split(":");
								type = typeArray[1];
							}
						}
						Map<String, List<Object>> paramTable = new Hashtable<String, List<Object>>();
						List<Object> typeList = new Vector<Object>();
						typeList.add(type);
						paramTable.put(Constants.ENTITY, typeList);
						paramQuery = Utility.fillParam(paramQuery, paramTable);
					}
					if(isDbQuery) {
						uris = getRawValues(mainEngine, paramQuery);
					} else {
						uris = getRawValues(mainEngine.getBaseDataEngine(), paramQuery);
					}
				} else { 
					// anything that is get Entity of Type must be on db
					uris = mainEngine.getEntityOfType(type);
				}
			}
		}
		return uris;
	}
	
	/**
	 * Flush out results
	 * @param engine
	 * @param query
	 * @return
	 */
	public List<Object> getRawValues(IDatabaseEngine engine, String query) {
		List<Object> ret = new Vector<Object>();
		IRawSelectWrapper wrap = null;
		try {
			wrap = WrapperManager.getInstance().getRawWrapper(engine, query);
			while(wrap.hasNext()) {
				Object value = wrap.next().getRawValues()[0];
				if(value instanceof Number) {
					ret.add(value);
				} else {
					ret.add(value.toString());
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrap != null) {
				try {
					wrap.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return ret;
	}
}
