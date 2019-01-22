package prerna.query.querystruct.evaluator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.RawGemlinSelectWrapper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.om.HeadersDataRow;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.AbstractWrapper;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

public class QueryStructExpressionIterator extends AbstractWrapper implements IRawSelectWrapper {

	/**
	 * It is really difficult to figure out the traversals in order to perform multiple math routines
	 * on columns... granted, I am dumb, so if you can figure it out, please fix the GremlinInterpreter class
	 * 
	 * So if there is math or something else that needs to be done, we are doing it programmatically through Java
	 */
	
	private List<IHeadersDataRow> processedData;
	private int processedDataSize;
	private int processedDataPosition;

	private IRawSelectWrapper subIt;
	private SelectQueryStruct qs;

	public QueryStructExpressionIterator(IRawSelectWrapper subIt, SelectQueryStruct qs) {
		this.subIt = subIt;
		this.qs = qs;
	}

	@Override
	public void execute() {
		// first, see that there is math that is needed
		List<IQuerySelector> selectors = this.qs.getSelectors();
		int numSelectors = selectors.size();

		List<String> uniqueSelectorNames = new ArrayList<String>(numSelectors);
		List<Integer> mathIndex = new ArrayList<Integer>(numSelectors);
		List<String> mathOperation = new ArrayList<String>(numSelectors);
		List<Integer> groupByIndex = new ArrayList<Integer>(numSelectors);
		String[] headers = new String[numSelectors];

		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				QueryFunctionSelector mSelector = (QueryFunctionSelector) selector;
				mathIndex.add(i);
				mathOperation.add(mSelector.getFunction());
			}
			uniqueSelectorNames.add(selector.getQueryStructName());
			headers[i] = selector.getAlias();
		}

		List<QueryColumnSelector> groups = this.qs.getGroupBy();
		int numGroups = groups.size();
		for(int i = 0; i < numGroups; i++) {
			QueryColumnSelector gSelector = groups.get(i);
			groupByIndex.add(uniqueSelectorNames.indexOf(gSelector.getQueryStructName()));
		}

		if(!mathIndex.isEmpty()) {
			// we need to process through this iterator to get our results
			calculateProcessedData(headers, uniqueSelectorNames, mathIndex, mathOperation, groupByIndex);
			this.rawHeaders = headers;
			this.headers = headers;
			this.numColumns = headers.length;
			this.types = new SemossDataType[this.numColumns];
			SemossDataType[] origTypes = this.subIt.getTypes();
			for(int i = 0; i < this.numColumns; i++) {
				if(groupByIndex.contains(new Integer(i))) {
					this.types[i] = SemossDataType.DOUBLE;
				} else {
					this.types[i] = origTypes[i];
				}
			}
		}
	}
	
	private void calculateProcessedData(String[] retHeaders,
			List<String> uniqueSelectorNames, 
			List<Integer> mathIndex, 
			List<String> mathOperation, 
			List<Integer> groupByIndex) {
		this.processedData = new Vector<IHeadersDataRow>();
		int numSelectors = uniqueSelectorNames.size();
		int numMathIndex = mathIndex.size();
		int numGroups = groupByIndex.size();

		// need to use a map so i can group things
		Map<String, List<IQueryStructExpression>> store = new LinkedHashMap<String, List<IQueryStructExpression>>();
		while(subIt.hasNext()) {
			Object[] dataRow = subIt.next().getValues();
			String gStr = groupByStr(dataRow, numGroups, groupByIndex);
			
			List<IQueryStructExpression> valuesStore = null;
			if(store.containsKey(gStr)) {
				valuesStore = store.get(gStr);
			} else {
				valuesStore = new ArrayList<IQueryStructExpression>();
				for(int i = 0; i < numMathIndex; i++) {
					valuesStore.add(IQueryStructExpression.getExpression(mathOperation.get(i)));
				}
				store.put(gStr, valuesStore);
			}
			
			for(int counter = 0; counter < numMathIndex; counter++) {
				Integer mathColIdx = mathIndex.get(counter);
				valuesStore.get(counter).processData(dataRow[mathColIdx]);
			}
		}
		
		// now i need to flatten this and preserve the headers
		for(String groupByStr : store.keySet()) {
			// add the group by indices
			Object[] row = new Object[numSelectors];
			if(numGroups == 1) {
				row[groupByIndex.get(0)] = groupByStr;
			} else {
				String[] split = groupByStr.split("\\+\\+\\+");
				for(int i = 0; i < numGroups; i++) {
					row[groupByIndex.get(i)] = split[i];
				}
			}
			
			List<IQueryStructExpression> calculations = store.get(groupByStr);
			// add the columns which had derivations
			for(int i = 0; i < numMathIndex; i++) {
				row[mathIndex.get(i)] = calculations.get(i).getOutput();
			}
			
			// add to processed data
			this.processedData.add(new HeadersDataRow(retHeaders, row));
		}
		this.processedDataSize = this.processedData.size();
	}
	
	private String groupByStr(Object[] dataRow, int numGroups, List<Integer> groupByIndex) {
		StringBuilder gStr = new StringBuilder();
		for(int i = 0; i < numGroups; i++) {
			int index = groupByIndex.get(i);
			if(i == 0) {
				gStr.append(dataRow[index]);
			} else {
				gStr.append("+++").append(dataRow[index]);

			}
		}
		return gStr.toString();
	}

	@Override
	public boolean hasNext() {
		if(processedData == null) {
			return this.subIt.hasNext();
		} else {
			if(processedDataPosition + 1 > processedDataSize) {
				return false;
			}
			return true;
		}
	}

	@Override
	public IHeadersDataRow next() {
		if(processedData == null) {
			return this.subIt.next();
		} else {
			IHeadersDataRow nextRow = processedData.get(processedDataPosition);
			processedDataPosition++;
			return nextRow;
		}
	}

	@Override
	public long getNumRecords() {
		if(processedData == null) {
			return this.subIt.getNumRecords();
		}
		return processedDataSize;
	}

	@Override
	public void cleanUp() {
		this.subIt.cleanUp();
	}

	@Override
	public void reset() {
		cleanUp();
		this.subIt.reset();
		execute();
	}

	@Override
	public String[] getHeaders() {
		if(this.headers == null) {
			return this.subIt.getHeaders();
		}
		return this.headers;
	}

	@Override
	public SemossDataType[] getTypes() {
		if(this.types == null) {
			return this.subIt.getTypes();
		}
		return this.types;
	}

	@Override
	public void setQuery(String query) {
		// this class doesn't set a query
		// it uses an existing datsource iterator
	}

	
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	/*
	 * Main for testing
	 */
	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		{
			String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
			IEngine coreEngine = new RDBMSNativeEngine();
			coreEngine.setEngineId("LocalMasterDatabase");
			coreEngine.openDB(engineProp);
			coreEngine.setEngineId("LocalMasterDatabase");
			DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);
		}
		
	
		String testEngine = "TinkerThis__cc2a91eb-548d-4970-91c3-7a043b783841";
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + testEngine + ".smss";
		TinkerEngine coreEngine = new TinkerEngine();
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(testEngine, coreEngine);
		
		
		GremlinInterpreter interp = new GremlinInterpreter(coreEngine.getGraph().traversal(), 
				coreEngine.getTypeMap(), coreEngine.getNameMap());
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("Studio"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.MEAN);
		fun.addInnerSelector(new QueryColumnSelector("Title__MovieBudget"));
		qs.addSelector(fun);
		qs.addGroupBy(new QueryColumnSelector("Studio"));
		qs.addRelation("Title", "Studio", "inner.join");
		
		RawGemlinSelectWrapper subIt = new RawGemlinSelectWrapper(interp, qs);
		subIt.execute();
		
		QueryStructExpressionIterator it = new QueryStructExpressionIterator(subIt, qs);
		it.execute();
		while(it.hasNext()) {
			System.out.println(it.next());
		}
	}
	
}
