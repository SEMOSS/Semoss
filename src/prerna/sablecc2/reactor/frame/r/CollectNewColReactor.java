package prerna.sablecc2.reactor.frame.r;

import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.poi.main.HeadersException;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Utility;

public class CollectNewColReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	AbstractRJavaTranslator rJavaTranslator = null;
	
	private static Map<String, String> mathMap = new HashMap<String, String>();
	static {
		mathMap.put("Sum", "sum");
		mathMap.put("Average", "mean");
		mathMap.put("Min", "min");
		mathMap.put("Max", "max");
		mathMap.put("Median", "median");
		mathMap.put("StandardDeviation", "sd");
		mathMap.put("Count", "N");
	};

	public CollectNewColReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey() };
	}

	public NounMetadata execute() {
		
		this.task = getTask();

		RDataTable frame = (RDataTable)this.insight.getCurFrame();
		rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		
		// get the query struct
		SelectQueryStruct sqs = ((BasicIteratorTask)task).getQueryStruct();
		

		OwlTemporalEngineMeta metadata = frame.getMetaData();
		SelectQueryStruct pqs = QSAliasToPhysicalConverter.getPhysicalQs(sqs, metadata);
		RInterpreter interp = new RInterpreter();
		interp.setQueryStruct(pqs);
		interp.setDataTableName(frame.getName());
		interp.setColDataTypes(metadata.getHeaderToTypeMap());
//		interp.setAdditionalTypes(this.metaData.getHeaderToAdtlTypeMap());
		interp.setLogger(this.getLogger(this.getClass().getName()));
		String query = interp.composeQuery();
		
		
		// query comes to this
		//tempaqBZQYvZoVE <- unique(mv[ , { V0=(RevenueInternational * 2) ; list(new_col=V0) }]);

		// need to drop all the way to unique
		// and then add it as a var on the frame
	
		String mainQuery = query.split("<-")[1];
		mainQuery = mainQuery.replace("unique", "");
		
		// need to get the query struct
		// there should be only one selector
		List <IQuerySelector> allSelectors = sqs.getSelectors();

		List<NounMetadata> outputs = new Vector<NounMetadata>(2);
		
		if(allSelectors.size() > 0)
		{
			IQuerySelector onlySelector = allSelectors.get(0);
			
			String alias = onlySelector.getAlias();
			mainQuery = frame.getName() + "$" + alias + "  <- " + mainQuery;
			rJavaTranslator.executeEmptyR(mainQuery);
			
			OwlTemporalEngineMeta meta =  genNewMetaFromVariable(frame.getName());
			
			// replace the meta in the R Data table
			((RDataTable)this.insight.getCurFrame()).setMetaData(meta);
			outputs.add(new NounMetadata("Added Col " + alias, PixelDataType.CONST_STRING));
			outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
		}	
		else
			outputs.add(new NounMetadata("No New Columns to add", PixelDataType.CONST_STRING));


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

	@Override
	protected void buildTask() {
		// do nothing
		
	}
	
	protected OwlTemporalEngineMeta genNewMetaFromVariable(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = getColumns(frameName);
		String[] colTypes = getColumnTypes(frameName);
		//clean headers
		HeadersException headerChecker = HeadersException.getInstance();
		colNames = headerChecker.getCleanHeaders(colNames);
		// update frame header names in R
		String rColNames = "";
		for (int i = 0; i < colNames.length; i++) {
			rColNames += "\"" + colNames[i] + "\"";
			if (i < colNames.length - 1) {
				rColNames += ", ";
			}
		}
		String script = "colnames(" + frameName + ") <- c(" + rColNames + ")";
		this.rJavaTranslator.executeEmptyR(script);
		
		OwlTemporalEngineMeta meta = new OwlTemporalEngineMeta();
		ImportUtility.parseTableColumnsAndTypesToFlatTable(meta, colNames, colTypes, frameName);
		return meta;
	}
	
	/**
	 * This method is used to get the column names of a frame
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		return this.rJavaTranslator.getColumns(frameName);
	}

	/**
	 * This method is used to get the column types of a frame
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		return this.rJavaTranslator.getColumnTypes(frameName);
	}



}
