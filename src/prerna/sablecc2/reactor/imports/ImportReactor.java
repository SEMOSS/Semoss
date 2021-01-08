package prerna.sablecc2.reactor.imports;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.SQLQueryUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.usertracking.UserTrackerFactory;

public class ImportReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(NativeImporter.class);
	private static final String CLASS_NAME = ImportReactor.class.getName();
	
	public ImportReactor() {
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
		
		// if we are loading from a native frame 
		// that is backed by a RDBMSNativeEngine
		// we will flush the query into a HQS
		if( (qs.getQsType() == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY || qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) 
				&& qs.getFrame().getFrameType() == DataFrameTypeEnum.NATIVE
				&& frame.getFrameType() == DataFrameTypeEnum.NATIVE) {
			NativeFrame queryFrame = (NativeFrame) qs.getFrame();
			// make sure it is RDBMSNativeEngine
			if(queryFrame.getQueryStruct().retrieveQueryStructEngine() instanceof RDBMSNativeEngine) {
			
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, qs.getFrame().getMetaData());
				NativeFrame newFrame = SQLQueryUtils.subQuery(queryFrame.getQueryStruct(), qs);
				newFrame.setName(frame.getName());
				newFrame.setLogger(logger);
	
				NounMetadata frameNoun = new NounMetadata(newFrame, PixelDataType.FRAME, 
						PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	
				// replace newFrame with the current frame references
				VarStore varStore = this.insight.getVarStore();
				Set<String> allFrameReferences = varStore.findAllVarReferencesForFrame(frame);
				for(String newAlias : allFrameReferences) {
					varStore.put(newAlias, frameNoun);
				}
				// close the old frame - dont need it anymore as its replaced with this new one
				frame.close();
				
				// track GA data
				UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);
				
				return frameNoun;
			}
		}
		
		// we are not having the special native frame case
		IRawSelectWrapper it = null;
		if(!(frame instanceof NativeFrame)) {
			try {
				it = ImportUtility.generateIterator(qs, frame);
			} catch(SemossPixelException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				String message = "Error occured executing query to load into the frame";
				if(e.getMessage() != null && !e.getMessage().isEmpty()) {
					message += ". " + e.getMessage();
				}
				throw new SemossPixelException(getError(message));
			}
			try {
				if(!ImportSizeRetrictions.importWithinLimit(frame, it)) {
					SemossPixelException exception = new SemossPixelException(
							new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
									PixelDataType.CONST_STRING, 
									PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
					exception.setContinueThreadOfExecution(false);
					throw exception;
				}
			} catch (SemossPixelException e) {
				throw e;
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(getError("Error occured executing query before loading into frame"));
			}
		}
		
		// insert the data
		IImporter importer = ImportFactory.getImporter(frame, qs, it);
		try {
			importer.insertData();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		}
		// need to clear the unique col count used by FE for determining the need for math
		frame.clearCachedMetrics();
		frame.clearQueryCache();
		
		if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.CSV_FILE) {
			storeCsvFileMeta((CsvQueryStruct) qs);
		} else if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.EXCEL_FILE) {
			storeExcelFileMeta((ExcelQueryStruct) qs);
		}
		
		// track GA data
		UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	protected SelectQueryStruct getQueryStruct() {
		GenRowStruct allNouns = this.store.getNoun(PixelDataType.QUERY_STRUCT.getKey());
		SelectQueryStruct queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return (SelectQueryStruct)object.getValue();
		} else {
			NounMetadata result = this.planner.getVariableValue("$RESULT");
			if(result.getNounType() == PixelDataType.QUERY_STRUCT) {
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
		
		// put this into the noun store
		// so that we can pull it for other pipeline
		ITableDataFrame defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
		this.store.makeNoun(ReactorKeysEnum.FRAME.getKey()).add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
		return defaultFrame;
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


