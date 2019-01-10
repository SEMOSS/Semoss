package prerna.sablecc2.reactor.imports;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.usertracking.UserTrackerFactory;

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
		SelectQueryStruct qs = getQueryStruct();
		ITableDataFrame frame = getFrame();
		// setting a default frame of native
		if(frame == null) {
			logger.info("No frame is defined. Generating a defualt of native frame");
			frame = new NativeFrame();
		}
		// set the logger into the frame
		frame.setLogger(logger);
		
		IRawSelectWrapper it = null;
		if(!(frame instanceof NativeFrame)) {
			try {
				it = ImportUtility.generateIterator(qs, frame);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(
						new NounMetadata("Error occured executing query before loading into frame", 
								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			if(!ImportSizeRetrictions.importWithinLimit(frame, it)) {
				SemossPixelException exception = new SemossPixelException(
						new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
								PixelDataType.CONST_STRING, 
								PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		}
		
		// insert the data
		IImporter importer = ImportFactory.getImporter(frame, qs, it);
		importer.insertData();
		// need to clear the unique col count used by FE for determining the need for math
		frame.clearCachedInfo();
		
		if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE) {
			storeCsvFileMeta((CsvQueryStruct) qs);
		} else if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			storeExcelFileMeta((ExcelQueryStruct) qs);
		}
		
		// track GA data
		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		retNoun.addAdditionalReturn(new NounMetadata(frame.getMetaData().getTableHeaderObjects(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS));
		return retNoun;
	}

	protected SelectQueryStruct getQueryStruct() {
		GenRowStruct allNouns = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
		SelectQueryStruct queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (SelectQueryStruct)object.getValue();
		} else {
			NounMetadata result = this.planner.getVariableValue("$RESULT");
			if(result.getNounType().equals("QUERYSTRUCT")) {
				queryStruct = (SelectQueryStruct)result.getValue();
			}
		}
		return queryStruct;
	}
	
	private ITableDataFrame getFrame() {
		// try specific key
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[1]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			NounMetadata noun = frameGrs.getNoun(0);
			if(noun.getNounType() == PixelDataType.FRAME) {
				return (ITableDataFrame) noun.getValue();
			}
			throw new IllegalArgumentException("Input in frame key (" + noun.getValue().toString() + ") is not a valid frame");
		}
		
		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}
		
		return (ITableDataFrame) this.insight.getDataMaker();
	}

	private void storeCsvFileMeta(CsvQueryStruct qs) {
		if(qs.getSource() == CsvQueryStruct.ORIG_SOURCE.FILE_UPLOAD) {
			FileMeta fileMeta = new FileMeta();
			fileMeta.setFileLoc(qs.getFilePath());
			fileMeta.setDataMap(qs.getColumnTypes());
			fileMeta.setNewHeaders(qs.getNewHeaderNames());
			fileMeta.setPixelString(this.originalSignature);
			fileMeta.setSelectors(qs.getSelectors());
			fileMeta.setAdditionalTypes(qs.getAdditionalTypes());
			fileMeta.setType(FileMeta.FILE_TYPE.CSV);
			this.insight.addFileUsedInInsight(fileMeta);
		} else {
			// it is from an API call of some sort
			// delete it
			// when we save, we want to repull every time
			File csvFile = new File(qs.getFilePath());
			csvFile.delete();
		}
	}
	
	private void storeExcelFileMeta(ExcelQueryStruct qs) {
		FileMeta fileMeta = new FileMeta();
		fileMeta.setFileLoc(qs.getFilePath());
		fileMeta.setDataMap(qs.getColumnTypes());
		fileMeta.setSheetName(qs.getSheetName());
		fileMeta.setNewHeaders(qs.getNewHeaderNames());
		fileMeta.setSelectors(qs.getSelectors());
		fileMeta.setAdditionalTypes(qs.getAdditionalTypes());
		fileMeta.setPixelString(this.originalSignature);
		fileMeta.setType(FileMeta.FILE_TYPE.EXCEL);
		this.insight.addFileUsedInInsight(fileMeta);
	}
}


