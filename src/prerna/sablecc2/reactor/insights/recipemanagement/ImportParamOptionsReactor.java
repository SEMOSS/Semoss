package prerna.sablecc2.reactor.insights.recipemanagement;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.om.Insight;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.query.parsers.SqlParser2;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSParseParamStruct;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.translations.ImportQueryTranslation;

public class ImportParamOptionsReactor extends AbstractReactor {

	public static final String PARAM_OPTIONS = "PARAM_OPTIONS";
	
	@Override
	public NounMetadata execute() {
		PixelList pixelList = this.insight.getPixelList();
		
		Insight tempInsight = new Insight();
		ImportQueryTranslation translation = new ImportQueryTranslation(tempInsight);
		// loop through recipe
		for(Pixel pixel : pixelList) {
			try {
				String pixelId = pixel.getId();
				String expression = pixel.getPixelString();
				translation.setPixelObj(pixel);
				expression = PixelPreProcessor.preProcessPixel(expression.trim(), new ArrayList<String>(), new HashMap<String, String>());
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation.
				tree.apply(translation);
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		
		Map<Pixel, SelectQueryStruct> imports = translation.getImportQsMap();
		// for each import
		// we need to get the proper param struct
		List<Map<String, Object>> params = new Vector<>();
		for(Pixel pixelStep : imports.keySet()) {
			SelectQueryStruct qs = imports.get(pixelStep);
			List<ParamStruct> paramList = getParamsForImport(imports.get(pixelStep), pixelStep);
			Map<String, Map <String, Map<String, Map<String, List<ParamStruct>>>>> paramOutput = organizeStruct(paramList);
			
			Map<String, Object> output = new HashMap<>();
			if(qs instanceof HardSelectQueryStruct) {
				output.put("baseQsType", "hqs");
			} else {
				output.put("baseQsType", "sqs");
			}
			output.put("qsType", qs.getQsType());
			output.put("appId", qs.getEngineId());
			output.put("pixelId", pixelStep.getId());
			output.put("pixelString", pixelStep.getPixelString());
			output.put("params", paramOutput);
			params.add(output);
		}
		
		NounMetadata retMap = new NounMetadata(params, PixelDataType.VECTOR);
//		this.insight.getVarStore().put(PARAM_OPTIONS, retMap);
		return retMap;
	}
	
	private List<ParamStruct> getParamsForImport(SelectQueryStruct qs, Pixel pixelObj) {
		List<ParamStruct> paramList = new Vector<>();
		
		if(qs instanceof HardSelectQueryStruct || qs.getCustomFrom() != null) {
			// do the logic of getting the params. The only issue here is
			// we assume the latest level which may not be true
			// but let us see
			String query = qs.getCustomFrom();
			if(query == null && qs instanceof HardSelectQueryStruct)
				query = ((HardSelectQueryStruct)qs).getQuery();
			SqlParser2 sqp2 = new SqlParser2();
			sqp2.parameterize = false;
			try {
				GenExpressionWrapper wrapper = sqp2.processQuery(query);
				Iterator <ParamStructDetails> structIterator = wrapper.paramToExpressionMap.keySet().iterator();
				while(structIterator.hasNext()) {
					ParamStructDetails nextStructDetails = structIterator.next();
					nextStructDetails.setBaseQsType("hqs");
					nextStructDetails.setAppId(qs.getEngineId());
					nextStructDetails.setPixelId(pixelObj.getId());
					nextStructDetails.setPixelString(pixelObj.getPixelString());
					ParamStruct nextStruct = new ParamStruct();
					nextStruct.addParamStructDetails(nextStructDetails);
					paramList.add(nextStruct);
				}
				// dont save it for now
				//insight.getVarStore().put(QS_WRAPPER, new NounMetadata(wrapper, PixelDataType.CUSTOM_DATA_STRUCTURE));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			// get the filters first
			GenRowFilters importFilters = qs.getExplicitFilters();
			Set<String> filteredColumns = importFilters.getAllQsFilteredColumns();

			for(IQueryFilter filter : importFilters) {
				QSParseParamStruct.parseFilter(filter, paramList);
			}
			
			// the above should be the filtered options
			// lets go through the selectors
			// and what is not filtered will be added as well
			List<String> addedQs = new Vector<>();
			List<IQuerySelector> selectors = qs.getSelectors();
			for(IQuerySelector select : selectors) {
				List<QueryColumnSelector> allColumnSelectors = select.getAllQueryColumns();
				for(QueryColumnSelector colS : allColumnSelectors) {
					
					String colQS = colS.getQueryStructName();
					if(filteredColumns.contains(colQS)) {
						// already have a filter on it
						continue;
					}
					if(addedQs.contains(colQS)) {
						// we have already added this
						continue;
					}
					
					ParamStructDetails detailsStruct = new ParamStructDetails();
					detailsStruct.setBaseQsType("sqs");
					detailsStruct.setAppId(qs.getEngineId());
					detailsStruct.setPixelId(pixelObj.getId());
					detailsStruct.setPixelString(pixelObj.getPixelString());
					detailsStruct.setTableName(colS.getTable());
					detailsStruct.setColumnName(colS.getColumn());
					detailsStruct.setOperator("==");
					ParamStruct pStruct = new ParamStruct();
					pStruct.setMultiple(true);
					pStruct.setSearchable(true);
					pStruct.addParamStructDetails(detailsStruct);
					paramList.add(pStruct);
					// store that this qs has been added
					addedQs.add(colQS);
				}
			}
		}
		
		return paramList;
	}
	
	public Map<String, Map <String, Map<String, Map<String, List<ParamStruct>>>>> organizeStruct(List <ParamStruct> structs) {
		Map<String, Map <String, Map<String, Map<String, List<ParamStruct>>>>> columnMap = new HashMap<>();
		// level 1 - column name
		// column (key) -- List of tables (Value)
		// level 2 - column + table
		// column + table (key) - operator (value)
		// level 3 - column + table + operator
		// column + table + operator(key) - Param Struct(value)
		// level 4 - frames - dont know how to get to this but.. 
		
		for(int paramIndex = 0;paramIndex < structs.size();paramIndex++) {
			ParamStruct thisStruct = structs.get(paramIndex);
			// these structs will always only have 1 struct
			ParamStructDetails thisStructDetails = thisStruct.getDetailsList().get(0);
			String columnName = thisStructDetails.getColumnName();
			String tableName = thisStructDetails.getTableName();
			String opName = thisStructDetails.getOperator();
			String opuName = thisStructDetails.getuOperator();
			if(opuName == null) {
				opuName = opName;
			}
			
			// get the table
			Map <String, Map<String, Map<String, List<ParamStruct>>>> tableMap = null;
			if(columnMap.containsKey(columnName)) {
				tableMap = (Map <String, Map<String, Map<String, List<ParamStruct>>>>) columnMap.get(columnName);
			} else {
				tableMap = new HashMap <String, Map<String, Map<String, List<ParamStruct>>>>();
			}
			
			// get the operator from the table
			Map <String, Map<String, List <ParamStruct>>> opMap = null;
			if(tableMap.containsKey(tableName)) {
				opMap = (Map <String, Map<String, List <ParamStruct>>>)tableMap.get(tableName);
			} else {
				opMap = new HashMap<String, Map<String, List<ParamStruct>>>();
			}
			
			// get the table unique operator
			Map<String, List <ParamStruct>> opuMap = null;
			if(opMap.containsKey(opName)) {
				opuMap = (Map<String, List <ParamStruct>>)opMap.get(opName);
			} else {
				opuMap = new HashMap<String, List<ParamStruct>>();
			}
			
			// get the actual paramstruct
			List <ParamStruct> curList = null;
			if(opuMap.containsKey(opuName)) {
				curList = (List <ParamStruct>) opuMap.get(opuName);
			} else {
				curList = new ArrayList<ParamStruct>();
			}
			
			// add the paramstruct
			curList.add(thisStruct);
			// add the opumap
			opuMap.put(opuName, curList);
			// add it to the operator
			opMap.put(opName, opuMap);
			// put the table
			tableMap.put(tableName, opMap);
			// put the column
			columnMap.put(columnName, tableMap);
		}
		
		return columnMap;
	}
	
}
