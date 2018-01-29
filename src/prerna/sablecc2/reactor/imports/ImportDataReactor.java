package prerna.sablecc2.reactor.imports;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ga.GATracker;

public class ImportDataReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ImportDataReactor.class.getName();
	
	public ImportDataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey(), ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// set the logger into the frame
		Logger logger = getLogger(CLASS_NAME);
		// this is greedy execution
		// will not return anything
		// but will update the frame in the pixel planner
		QueryStruct2 qs = getQueryStruct();
		ITableDataFrame frame = getFrame();
		// setting a default frame of native
		if(frame == null) {
			logger.info("No frame is defined. Generating a defualt of native frame");
			frame = new NativeFrame();
		}

		// set the logger into the frame
		frame.setLogger(logger);
				
		// track GA data
		GATracker.getInstance().trackDataImport(this.insight, qs);
		
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


