package prerna.reactor.imports;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.InsightFile;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.ParquetQueryStruct;
import prerna.query.querystruct.SQLQueryUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
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
		
		QUERY_STRUCT_TYPE thisImportQsType = qs.getQsType();
		boolean limitRawQuery = false;
		long limitRawQueryVal = -1;
		if(thisImportQsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY || thisImportQsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY
				|| thisImportQsType == QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY || thisImportQsType == QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY) {
			if( (limitRawQueryVal=qs.getLimit()) > 0) {
				limitRawQuery = true;
			}
		}
		
		// if we are loading from a native frame 
		// that is backed by a RDBMSNativeEngine
		// we will flush the query into a HQS
		if( (qs.getQsType() == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY || qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) 
				&& qs.getFrame().getFrameType() == DataFrameTypeEnum.NATIVE
				&& frame.getFrameType() == DataFrameTypeEnum.NATIVE) {
			NativeFrame queryFrame = (NativeFrame) qs.getFrame();
			// make sure it is RDBMSNativeEngine
			IDatabaseEngine engine = queryFrame.getQueryStruct().retrieveQueryStructEngine();
			if(engine instanceof IRDBMSEngine) {
				NativeFrame newFrame = SQLQueryUtils.subQueryNativeFrame(queryFrame.prepQsForExecution(qs));
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
		BasicIteratorTask task = null;
		IRawSelectWrapper it = null;
		try {
			if(!(frame instanceof NativeFrame)) {
				try {
					it = ImportUtility.generateIterator(qs, frame);
				} catch(SemossPixelException e) {
					throw e;
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					String message = "Error occurred executing query to load into the frame";
					if(e.getMessage() != null && !e.getMessage().isEmpty()) {
						message += ". " + e.getMessage();
					}
					throw new SemossPixelException(getError(message));
				}
				
				// is there an additional limit on a raw query?
				if(limitRawQuery) {
					if(!FrameSizeRetrictions.sizeWithinLimit(limitRawQueryVal)) {
						SemossPixelException exception = new SemossPixelException(
								new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
										PixelDataType.CONST_STRING, 
										PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
						exception.setContinueThreadOfExecution(false);
						throw exception;
					}
					// set the limit into the flushed iterator
					task = new BasicIteratorTask(qs, it);
					task.setCollectLimit(limitRawQueryVal);
				} else {
					try {
						if(!FrameSizeRetrictions.importWithinLimit(frame, it)) {
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
						throw new SemossPixelException(getError("Error occurred executing query before loading into frame"));
					}
				}
			}
			
			// insert the data
			IImporter importer = null;
			if(task != null) {
				importer = ImportFactory.getImporter(frame, qs, task);
			} else {
				importer = ImportFactory.getImporter(frame, qs, it);
			}
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
			} else if(qs.getQsType() == SelectQueryStruct.QUERY_STRUCT_TYPE.PARQUET_FILE) {
				storeParquetFileMeta((ParquetQueryStruct) qs);
			}
			
			// track GA data
			UserTrackerFactory.getInstance().trackDataImport(this.insight, qs);
			
			return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		} finally {
			// always clean up the iterator
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(task != null) {
				try {
					task.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
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
			InsightFile insightFile = new InsightFile();
			insightFile.setFilePath(qs.getFilePath());
			insightFile.setFrameUpload(true);
			this.insight.addLoadInsightFile(insightFile);
		} else {
			// it is from an API call of some sort
			// delete it
			// when we save, we want to repull every time
			File csvFile = new File(qs.getFilePath());
			csvFile.delete();
		}
	}
	
	private void storeExcelFileMeta(ExcelQueryStruct qs) {
		InsightFile insightFile = new InsightFile();
		insightFile.setFilePath(qs.getFilePath());
		insightFile.setFrameUpload(true);
		this.insight.addLoadInsightFile(insightFile);
	}
	
	private void storeParquetFileMeta(ParquetQueryStruct qs) {
		// based on convo w/ PK, this could happen
		if(qs.getSource() == ParquetQueryStruct.ORIG_SOURCE.FILE_UPLOAD) {
			InsightFile insightFile = new InsightFile();
			insightFile.setFilePath(qs.getFilePath());
			insightFile.setFrameUpload(true);
			this.insight.addLoadInsightFile(insightFile);
		} else {
			// it is from an API call of some sort
			// delete it
			// when we save, we want to repull every time
			File parquetFile = new File(qs.getFilePath());
			parquetFile.delete();
		}
	}
}


