package prerna.reactor.export;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
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
		IDatabaseEngine engine = qs.retrieveQueryStructEngine();
		if(engine == null) {
			throw new IllegalArgumentException("Can only predict the row count for Basic Iterators - currently do not handle map operations");
		}
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(engine, qs, true);
			long start = System.currentTimeMillis();
			logger.info("Query Row Count : Executing query on engine " + engine.getEngineId());
			long numRows = iterator.getNumRows();
			long end = System.currentTimeMillis();
			logger.info("Query Row Count : Engine execution time = " + (end-start) + "ms");
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
