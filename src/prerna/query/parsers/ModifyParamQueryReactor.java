package prerna.query.parsers;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;

public class ModifyParamQueryReactor extends AbstractQueryStructReactor {

	private static final Logger classLogger = LogManager.getLogger(RDBMSNativeEngine.class);

	public ModifyParamQueryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.QUERY_KEY.getKey()};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		String filterValue = this.keyValue.get(this.keysToGet[0]);
		// if no value passed, do nothing
		if(filterValue != null && !filterValue.isEmpty()) {
			if(this.qs instanceof HardSelectQueryStruct) {
				modifyHqs(filterValue);
			} else {
				modifySqs(filterValue);
			}
		}

		return this.qs;
	}
	
	/**
	 * Modify the SQS that is being used based on a search
	 * @param filterValue
	 */
	private void modifySqs(String filterValue) {
		// we will be applying the filter on the QS name
		String colQs = this.qs.getSelectors().get(0).getQueryStructName();
		SimpleQueryFilter newF = SimpleQueryFilter.makeColToValFilter(colQs, "?like", filterValue);
		this.qs.addExplicitFilter(newF);
	}

	/**
	 * Modify a HQS that is being used
	 * Assumes the query is SQL
	 * @param filterValue
	 */
	private void modifyHqs(String filterValue) {
		// grab the query
		String query = ((HardSelectQueryStruct) this.qs).getQuery();

		// execute query to get the alias name
		String updatedQuery = "select * from (" + query + ") t12345 where 1=0;";
		IDatabaseEngine engine = this.qs.retrieveQueryStructEngine();
		IRDBMSEngine rdbms = null;
		if(engine instanceof IRDBMSEngine) {
			rdbms = (IRDBMSEngine) engine;
		} else {
			throw new IllegalArgumentException("Engine must be of type RDBMS to use this reactor");
		}
		
		String columnName = null;
		boolean requireCast = false;
		IRawSelectWrapper it = null;
		try {
			it = WrapperManager.getInstance().getRawWrapper(engine, updatedQuery);
			String[] headers = it.getHeaders();
			SemossDataType[] types = it.getTypes();
			columnName = headers[0];
			if(columnName == null || columnName.isEmpty()) {
				throw new SemossPixelException("Please provide an alias for the param query in order to properly execute");
			}
			if(types[0] != SemossDataType.STRING) {
				requireCast = true;
			}
		} catch(SemossPixelException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error in executing the param query for the insight");
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		columnName = rdbms.getQueryUtil().getEscapeKeyword(columnName);
		String newQuery = null;
		if(requireCast) {
			newQuery = "select distinct " + columnName + " from (" 
				+ query + ") t12345 where LOWER(CAST(" + columnName 
				+ " AS CHAR(500))) LIKE '%" + filterValue.toLowerCase() + "%' order by " + columnName;
		} else {
			newQuery = "select distinct " + columnName + " from (" 
					+ query + ") t12345 where LOWER(" + columnName 
					+ ") LIKE '%" + filterValue.toLowerCase() + "%' order by " + columnName;
		}
		((HardSelectQueryStruct) this.qs).setQuery(newQuery);
	}
	
}
