package prerna.sablecc2.reactor.export;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class QueryRowCountReactor  extends AbstractReactor {

	private static final String CLASS_NAME = QueryRowCountReactor.class.getName();
	
	public QueryRowCountReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}
	
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		SelectQueryStruct qs = getQs();
		IEngine engine = qs.retrieveQueryStructEngine();
		if(engine == null) {
			throw new IllegalArgumentException("Can only predict the row count for Basic Iterators - currently do not handle map operations");
		}
		IRawSelectWrapper iterator = null;
		try {
			 iterator = WrapperManager.getInstance().getRawWrapper(engine, qs, true);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error occured retrieving the query with message " + e.getMessage());
		}
		
		try {
			long start = System.currentTimeMillis();
			logger.info("Executing query on engine " + engine.getEngineId());
			long numRows = iterator.getNumRows();
			long end = System.currentTimeMillis();
			logger.info("Engine execution time = " + (end-start) + "ms");
			return new NounMetadata(numRows, PixelDataType.CONST_INT);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error occured retrieving the count of the query with message " + e.getMessage());
		}
	}

	/**
	 * Generate the task from the query struct
	 * @return
	 */
	private SelectQueryStruct getQs() {
		NounMetadata noun = null;
		SelectQueryStruct qs = null;

		GenRowStruct grsQs = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
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
