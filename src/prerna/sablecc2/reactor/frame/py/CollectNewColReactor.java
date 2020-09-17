package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.ds.r.RDataTable;
import prerna.poi.main.HeadersException;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;

public class CollectNewColReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	PyTranslator pyt = null;
	

	public CollectNewColReactor() {

	}

	public NounMetadata execute() {
		
		this.task = getTask();

		PandasFrame frame = (PandasFrame)this.insight.getCurFrame();
		pyt = insight.getPyTranslator();
		
		// get the query struct
		SelectQueryStruct sqs = ((BasicIteratorTask)task).getQueryStruct();
		String warning = null;

		// need to get the query struct
		// there should be only one selector
		List <IQuerySelector> allSelectors = sqs.getSelectors();

		OwlTemporalEngineMeta metadata = frame.getMetaData();
		SelectQueryStruct pqs = QSAliasToPhysicalConverter.getPhysicalQs(sqs, metadata);
		if(pqs.getCombinedFilters().getFilters() != null && pqs.getCombinedFilters().getFilters().size() > 0 )
		{
			warning = "You are applying a calculation while there are filters, this is not recommended and can lead to unpredictable results";
			pqs.ignoreFilters = true;
		}		

		List<NounMetadata> outputs = new Vector<NounMetadata>();
		
		if(allSelectors.size() > 0)
		{
			IQuerySelector onlySelector = allSelectors.get(0);
			String mainQuery = processSelector(onlySelector, frame.getName()).toString();
			
			String alias = onlySelector.getAlias();
			mainQuery = frame.getName() + "['" + alias + "'] = " + mainQuery;
			pyt.runEmptyPy(mainQuery);
			
			recreateMetadata(frame, true);
			
			outputs.add(new NounMetadata("Added Col " + alias, PixelDataType.CONST_STRING));
			outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
			if(warning != null)
				outputs.add(getWarning(warning));
		}	
		else
			outputs.add(new NounMetadata("No New Columns to add", PixelDataType.CONST_STRING));


		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
	
	public String convertQSToFormula(IQuerySelector selector)
	{
		String retString = null;
		
		
		
		return retString;
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

	@Override
	protected void buildTask() {
		// do nothing
		
	}
	
	/// recreating the metadata
	protected ITableDataFrame recreateMetadata(PandasFrame frame, boolean override) {
		// grab the existing metadata from the frame
		Map<String, String> additionalDataTypes = frame.getMetaData().getHeaderToAdtlTypeMap();
		Map<String, List<String>> sources = frame.getMetaData().getHeaderToSources();
		
		String frameName = frame.getName();
		PandasFrame newFrame = frame; 
		// I am just going to try to recreate the frame here
		if(override) {
			newFrame = new PandasFrame(frameName);
			newFrame.setJep(frame.getJep());
			newFrame.setTranslator(insight.getPyTranslator());
			String makeWrapper = PandasSyntaxHelper.makeWrapper(newFrame.getWrapperName(), frameName);
			//newFrame.runScript(makeWrapper);
			insight.getPyTranslator().runEmptyPy(makeWrapper);
		}
		String[] colNames = getColumns(newFrame);
		// I bet this is being done for pixel.. I will keep the same
		//newFrame.runScript(PandasSyntaxHelper.cleanFrameHeaders(frameName, colNames));
		insight.getPyTranslator().runEmptyPy(PandasSyntaxHelper.cleanFrameHeaders(frameName, colNames));
		colNames = getColumns(newFrame);
		
		String[] colTypes = getColumnTypes(newFrame);
		if(colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + frameName + " exists and can be a valid data.table object");
		}
		
		// create the pandas frame
		// and set up everything else
		ImportUtility.parseTableColumnsAndTypesToFlatTable(newFrame.getMetaData(), colNames, colTypes, frameName, additionalDataTypes, sources);
		if (override) {
			this.insight.setDataMaker(newFrame);
		}
		// update varStore
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME);
		this.insight.getVarStore().put(frame.getName(), noun);
		
		return newFrame;
	}

	public String[] getColumns(PandasFrame frame) {		
		String wrapperName = frame.getWrapperName();
		// get jep thread and get the names
		List<String> val = (List<String>) insight.getPyTranslator().runScript("list(" + wrapperName + ".cache['data'])");
		return val.toArray(new String[val.size()]);
	}
	
	public String[] getColumnTypes(PandasFrame frame) {
		String wrapperName = frame.getWrapperName();
		// get jep thread and get the names
		List<String> val = (List<String>) insight.getPyTranslator().runScript(PandasSyntaxHelper.getTypes(wrapperName + ".cache['data']"));
		return val.toArray(new String[val.size()]);
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
		 * sorry no functions allowed
		else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
			return processAggSelector((QueryFunctionSelector) selector);
		}*/
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



}
