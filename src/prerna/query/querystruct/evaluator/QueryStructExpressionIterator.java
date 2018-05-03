package prerna.query.querystruct.evaluator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.AbstractWrapper;

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
	
	private Iterator<IHeadersDataRow> mainIterator;
	private SelectQueryStruct qs;
	
	// keep track of the math that is needed
	private List<String> uniqueSelectorNames;
	private List<Integer> mathIndex;
	private List<String> mathOperation;
	private List<Integer> groupByIndex;
	private String[] headers;
	/**
	 * Constructor
	 */
	public QueryStructExpressionIterator(Iterator<IHeadersDataRow> mainIterator, SelectQueryStruct qs) {
		this.mainIterator = mainIterator;
		this.qs = qs;
		init();
	}
	
	@Override
	public boolean hasNext() {
		if(processedData == null) {
			return this.mainIterator.hasNext();
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
			return this.mainIterator.next();
		} else {
			IHeadersDataRow nextRow = processedData.get(processedDataPosition);
			processedDataPosition++;
			return nextRow;
		}
	}
	
	public void init() {
		// first, see that there is math that is needed
		List<IQuerySelector> selectors = this.qs.getSelectors();
		int numSelectors = selectors.size();
				
		this.uniqueSelectorNames = new ArrayList<String>(numSelectors);
		this.mathIndex = new ArrayList<Integer>(numSelectors);
		this.mathOperation = new ArrayList<String>(numSelectors);
		this.groupByIndex = new ArrayList<Integer>(numSelectors);
		this.headers = new String[numSelectors];
		
		for(int i = 0; i < numSelectors; i++) {
			IQuerySelector selector = selectors.get(i);
			if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				QueryFunctionSelector mSelector = (QueryFunctionSelector) selector;
				this.mathIndex.add(i);
				this.mathOperation.add(mSelector.getFunction());
			}
			//TODO: doing the base caseO
			this.uniqueSelectorNames.add(selector.getQueryStructName());
			this.headers[i] = selector.getAlias();
		}
		
		List<QueryColumnSelector> groups = this.qs.getGroupBy();
		int numGroups = groups.size();
		for(int i = 0; i < numGroups; i++) {
			QueryColumnSelector gSelector = groups.get(i);
			this.groupByIndex.add(this.uniqueSelectorNames.indexOf(gSelector.getQueryStructName()));
		}
		
		if(!this.mathIndex.isEmpty()) {
			// we need to process through this iterator to get our results
			calculateProcessedData();
		}
	}

	private void calculateProcessedData() {
		this.processedData = new Vector<IHeadersDataRow>();
		int numSelectors = this.uniqueSelectorNames.size();
		int numMathIndex = this.mathIndex.size();
		int numGroups = this.groupByIndex.size();

		// need to use a map so i can group things
		Map<String, List<IQueryStructExpression>> store = new LinkedHashMap<String, List<IQueryStructExpression>>();
		while(this.mainIterator.hasNext()) {
			Object[] dataRow = mainIterator.next().getValues();
			String gStr = groupByStr(dataRow, numGroups);
			
			List<IQueryStructExpression> valuesStore = null;
			if(store.containsKey(gStr)) {
				valuesStore = store.get(gStr);
			} else {
				valuesStore = new ArrayList<IQueryStructExpression>();
				for(int i = 0; i < numMathIndex; i++) {
					valuesStore.add(IQueryStructExpression.getExpression(this.mathOperation.get(i)));
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
				row[this.groupByIndex.get(0)] = groupByStr;
			} else {
				String[] split = groupByStr.split("\\+\\+\\+");
				for(int i = 0; i < numGroups; i++) {
					row[this.groupByIndex.get(i)] = split[i];
				}
			}
			
			List<IQueryStructExpression> calculations = store.get(groupByStr);
			// add the columns which had derivations
			for(int i = 0; i < numMathIndex; i++) {
				row[this.mathIndex.get(i)] = calculations.get(i).getOutput();
			}
			
			// add to processed data
			this.processedData.add(new HeadersDataRow(this.headers, row));
		}
		this.processedDataSize = this.processedData.size();
	}
	
	private String groupByStr(Object[] dataRow, int numGroups) {
		StringBuilder gStr = new StringBuilder();
		for(int i = 0; i < numGroups; i++) {
			int index = this.groupByIndex.get(i);
			if(i == 0) {
				gStr.append(dataRow[index]);
			} else {
				gStr.append("+++").append(dataRow[index]);

			}
		}
		return gStr.toString();
	}
	
	@Override
	public void execute() {

	}
	
	@Override
	public void cleanUp() {
		
	}

	@Override
	public String[] getDisplayVariables() {
		return this.headers;
	}

	@Override
	public String[] getPhysicalVariables() {
		return this.headers;
	}

	@Override
	public String[] getTypes() {
		int size = this.headers.length;
		String[] types = new String[size];
		for(int i = 0; i < size; i++) {
			if(this.mathIndex.contains(new Integer(i))) {
				types[i] = "NUMBER";
			} else {
				types[i] = "STRING";
			}
		}
		return types;
	}
}
