package prerna.sablecc2.reactor.imports;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class ImportDataReactor extends AbstractReactor {
	
	public ImportDataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey(), ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// this is greedy execution
		// will not return anything
		// but will update the frame in the pixel planner
		QueryStruct2 qs = getQueryStruct();
		ITableDataFrame frame = getFrame();

		// set the logger into the frame
		Logger logger = getLogger(frame.getClass().getName());
		frame.setLogger(logger);
		
		// Format and send Google Analytics data
		String engine = qs.getEngineName() + "";
		// if the engine doesnt have a name then the data is coming from a temp table
		if (qs.getEngineName() == null){
			String tempFrameName = qs.getFrame() + "";
			engine = "TempFrame_" + tempFrameName ;
		}
		String curExpression = "";
		List<IQuerySelector> selectors = qs.getSelectors();
		for (int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			String columnSelected = "";
			if (selector instanceof QueryColumnSelector) {
				// we can get a table and column
				columnSelected = ((QueryColumnSelector) selector).getTable() + "__" + ((QueryColumnSelector) selector).getAlias();
			} else {
				// only alias
				columnSelected = selector.getAlias();
			}
			curExpression = curExpression + engine + ":" + columnSelected;
			if (i != (selectors.size() - 1)) {
				curExpression += ";";
			}
		}
		if (curExpression.equals("") && engine.equals("DIRECT_ENGINE_CONNECTION")){
			curExpression = curExpression + engine ;
		}
		insight.trackPixels("dataquery", curExpression);
	
		// insert the data
		IImporter importer = ImportFactory.getImporter(frame, qs);
		importer.insertData();
		// need to clear the unique col count used by FE for determining the need for math
		frame.clearCachedInfo();
		
		if(qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.CSV_FILE) {
			storeCsvFileMeta((CsvQueryStruct) qs);
		} else if(qs.getQsType() == QueryStruct2.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			storeExcelFileMeta((ExcelQueryStruct) qs);
		}
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	private QueryStruct2 getQueryStruct() {
		GenRowStruct allNouns = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
		QueryStruct2 queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (QueryStruct2)object.getValue();
		} else {
			NounMetadata result = this.planner.getVariableValue("$RESULT");
			if(result.getNounType().equals("QUERYSTRUCT")) {
				queryStruct = (QueryStruct2)result.getValue();
			}
		}
		return queryStruct;
	}
	
	private ITableDataFrame getFrame() {
		// try specific key
		GenRowStruct frameGrs = this.store.getNoun(keysToGet[1]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		return (ITableDataFrame) this.insight.getDataMaker();
	}

	private void storeCsvFileMeta(CsvQueryStruct qs) {
		FileMeta fileMeta = new FileMeta();
		fileMeta.setFileLoc(qs.getCsvFilePath());
		fileMeta.setDataMap(qs.getColumnTypes());
		fileMeta.setNewHeaders(qs.getNewHeaderNames());
		fileMeta.setPixelString(this.originalSignature);
		fileMeta.setSelectors(qs.getSelectors());
		fileMeta.setType(FileMeta.FILE_TYPE.CSV);
		this.insight.addFileUsedInInsight(fileMeta);
	}
	
	private void storeExcelFileMeta(ExcelQueryStruct qs) {
		FileMeta fileMeta = new FileMeta();
		fileMeta.setFileLoc(qs.getExcelFilePath());
		fileMeta.setDataMap(qs.getColumnTypes());
		fileMeta.setSheetName(qs.getSheetName());
		fileMeta.setNewHeaders(qs.getNewHeaderNames());
		fileMeta.setSelectors(qs.getSelectors());
		fileMeta.setPixelString(this.originalSignature);
		fileMeta.setType(FileMeta.FILE_TYPE.EXCEL);
		this.insight.addFileUsedInInsight(fileMeta);
	}
}


