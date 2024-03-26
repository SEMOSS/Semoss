package prerna.reactor.export;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class QueryRowCountReactor  extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(QueryRowCountReactor.class);
	private static final String CLASS_NAME = QueryRowCountReactor.class.getName();

	public QueryRowCountReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}

	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		SelectQueryStruct qs = getQs();
		QUERY_STRUCT_TYPE qsType = qs.getQsType();
		
		if(qsType == QUERY_STRUCT_TYPE.ENGINE
				|| qsType == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY
				|| qsType == QUERY_STRUCT_TYPE.RAW_JDBC_ENGINE_QUERY
				|| qsType == QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY) {
			return rowsForEngine(qs, logger);
		} else if(qsType == QUERY_STRUCT_TYPE.FRAME
				|| qsType == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) {
			return rowsForFrame(qs, logger);
		}
		
		throw new IllegalArgumentException("Can not determine row count for Query Struct of type " + qsType);
	}
	
	/**
	 * 
	 * @param qs
	 * @param logger
	 * @return
	 */
	private NounMetadata rowsForFrame(SelectQueryStruct qs, Logger logger) {
		ITableDataFrame frame = qs.getFrame();
		if(frame == null) {
			throw new IllegalArgumentException("Query Struct is of type " + qs.getQsType() + " but no frame is set");
		}
		IRawSelectWrapper iterator = null;
		try {
			iterator = frame.query(qs);
			long start = System.currentTimeMillis();
			logger.info("Query Row Count : Executing query on frame " + frame.getName());
			long numRows = iterator.getNumRows();
			long end = System.currentTimeMillis();
			logger.info("Query Row Count : Frame " + frame.getName() + " execution time = " + (end-start) + "ms");
			return new NounMetadata(numRows, PixelDataType.CONST_INT, PixelOperationType.QUERY_ROW_COUNT);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			if(iterator == null) {
				throw new IllegalArgumentException("Error occurred retrieving the query with message " + e.getMessage());
			} else {
				throw new IllegalArgumentException("Error occurred retrieving the count of the query with message " + e.getMessage());
			}
		} finally {
			if(iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * 
	 * @param qs
	 * @param logger
	 * @return
	 */
	private NounMetadata rowsForEngine(SelectQueryStruct qs, Logger logger) {
		IDatabaseEngine engine = qs.retrieveQueryStructEngine();
		if(engine == null) {
			throw new IllegalArgumentException("Query Struct is of type " + qs.getQsType() + " but no engine is set");
		}
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(engine, qs, true);
			long start = System.currentTimeMillis();
			logger.info("Query Row Count : Executing query on engine " + engine.getEngineId());
			long numRows = iterator.getNumRows();
			long end = System.currentTimeMillis();
			logger.info("Query Row Count : Engine " + engine.getEngineId() + " execution time = " + (end-start) + "ms");
			return new NounMetadata(numRows, PixelDataType.CONST_INT, PixelOperationType.QUERY_ROW_COUNT);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			if(iterator == null) {
				throw new IllegalArgumentException("Error occurred retrieving the query with message " + e.getMessage());
			} else {
				throw new IllegalArgumentException("Error occurred retrieving the count of the query with message " + e.getMessage());
			}
		} finally {
			if(iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Generate the task from the query struct
	 * @return
	 */
	private SelectQueryStruct getQs() {
		NounMetadata noun = null;
		SelectQueryStruct qs = null;

		GenRowStruct grsQs = this.store.getNoun(PixelDataType.QUERY_STRUCT.getKey());
		//if we don't have tasks in the curRow, check if it exists in genrow under the qs key
		if(grsQs != null && !grsQs.isEmpty()) {
			noun = grsQs.getNoun(0);
			qs = (SelectQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if(qsList != null && !qsList.isEmpty()) {
				noun = qsList.get(0);
				qs = (SelectQueryStruct) noun.getValue();
			}
		}

		if(qs == null) {
			throw new IllegalArgumentException("Must pass in a database query to get the row count");
		}

		return qs;
	}

}
