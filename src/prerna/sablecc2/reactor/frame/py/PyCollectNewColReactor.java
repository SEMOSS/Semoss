package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PyTranslator;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;

public class PyCollectNewColReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	PyTranslator pyt = null;

	public PyCollectNewColReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	public NounMetadata execute() {
		String warning = null;
		
		if(! ((this.task=getTask()) instanceof BasicIteratorTask)) {
			throw new IllegalArgumentException("Can only add a new column using a basic query on a frame");
		}
		
		// get the query struct
		SelectQueryStruct sqs = ((BasicIteratorTask) this.task).getQueryStruct();
		PandasFrame frame = (PandasFrame) sqs.getFrame();
		pyt = insight.getPyTranslator();
		
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		SelectQueryStruct pqs = null;

		try {
			// if the columns are not there. 
			pqs = QSAliasToPhysicalConverter.getPhysicalQs(sqs, metadata);
		} catch(Exception ex) {
			return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
		}

		if(pqs.getCombinedFilters().getFilters() != null && pqs.getCombinedFilters().getFilters().size() > 0 ) {
			warning = "You are applying a calculation while there are filters, this is not recommended and can lead to unpredictable results";
			pqs.ignoreFilters = true;
		}

		List<NounMetadata> outputs = new Vector<NounMetadata>();
		// there should be only one selector
		List <IQuerySelector> allSelectors = sqs.getSelectors();
		if(allSelectors.size() > 0) {
			IQuerySelector onlySelector = allSelectors.get(0);
			PandasInterpreter interp = new PandasInterpreter();
			interp.setDataTableName(frame.getName(), frame.getName() + "w.cache['data']");
			interp.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
			// I should also possibly set up pytranslator so I can run command for creating filter
			interp.setPyTranslator(pyt);
			String interpOutput = interp.processSelector(onlySelector, frame.getName(), false, false);

			String mainQuery = processSelector(onlySelector, frame.getName()).toString();
			
			String alias = onlySelector.getAlias();
			mainQuery = frame.getName() + "['" + alias + "'] = " + mainQuery;
			pyt.runEmptyPy(mainQuery);

			// recreate the metadata
			frame.recreateMeta();
			
			outputs.add(new NounMetadata("Added Col " + alias, PixelDataType.CONST_STRING));
			outputs.add(new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
			if(warning != null) {
				outputs.add(getWarning(warning));
			}
		} else {
			outputs.add(new NounMetadata("No New Columns to add", PixelDataType.CONST_STRING));
		}

		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null && !outputs.isEmpty()) return outputs;

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}

	// compose the expression
	// keep the expression here and build it
	
	private StringBuffer processSelector(IQuerySelector selector, String tableName) {
		SELECTOR_TYPE selectorType = selector.getSelectorType();
		
		if(selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector( (QueryColumnSelector) selector, tableName);
		}
		// constan is not touched yet
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} 
		/*
		 * sorry no functions allowed*/
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processFunctionSelector((QueryFunctionSelector) selector, tableName);
		}
		// arithmetic selector is not implemented
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector, tableName);
		} else {
			return null;
		}
	}

	private StringBuffer processColumnSelector(QueryColumnSelector selector, String tableName) {
		String columnName = selector.getColumn();
		String table = selector.getTable();
		String alias = selector.getAlias();
		
		// just return the column name
		return new StringBuffer(tableName).append("['").append(alias).append("']");
	}

	private StringBuffer processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof Number) {
			return new StringBuffer(constant.toString());
		} else {
			return new StringBuffer("'").append(constant).append("'");
		}
	}

	private StringBuffer processArithmeticSelector(QueryArithmeticSelector selector, String tableName) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		return new StringBuffer("(" + processSelector(leftSelector, tableName) + " " + mathExpr + " " + processSelector(rightSelector, tableName) + ")");
	}
	
	private StringBuffer processFunctionSelector(QueryFunctionSelector selector, String tableName)
	{
		//Sum(MovieBudget)
		StringBuffer retBuffer = new StringBuffer();
		// get the name of the column
		String functionName = selector.getFunction();
		List <IQuerySelector> paramSelectors = selector.getInnerSelector();
		// usually this is a single parameter, if it is more, I dont know what to do
		if(paramSelectors != null)
		{
			IQuerySelector curSelector = paramSelectors.get(0);
			if(curSelector instanceof QueryColumnSelector)
			{
				QueryColumnSelector cs = (QueryColumnSelector)curSelector;
				String columnName = cs.getAlias();
				functionName = QueryFunctionHelper.convertFunctionToPandasSyntax(functionName);
				retBuffer.append(tableName).append("['").append(columnName).append("'].").append(functionName).append("()");
			}
		}
		
		
		//System.err.println("Selector..  " + selector);
		
		
		return retBuffer;
	}

	@Override
	protected void buildTask() {
		// do nothing
		
	}
}
