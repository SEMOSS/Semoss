package prerna.query.parsers;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class ModifyParamQueryReactor extends AbstractQueryStructReactor {

	public ModifyParamQueryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILTER_WORD.getKey(), ReactorKeysEnum.QUERY_KEY.getKey()};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		if(!(this.qs instanceof HardSelectQueryStruct)) {
			throw new IllegalArgumentException("Can only modify a direct SQL query");
		}
		
		organizeKeys();
		String filterValue = this.keyValue.get(this.keysToGet[0]);
		// if no value passed, do nothing
		if(filterValue != null && !filterValue.isEmpty()) {
			// grab the query
			String query = ((HardSelectQueryStruct) this.qs).getQuery();

			// execute query to get the alias name
			String updatedQuery = "select * from (" + query + ") t12345 where 1=0;";
			IEngine engine = this.qs.retrieveQueryStructEngine();
			if(!(engine instanceof RDBMSNativeEngine)) {
				throw new IllegalArgumentException("Engine must be of type RDBMS to use this reactor");
			}
			RDBMSNativeEngine rdbms = (RDBMSNativeEngine) engine;
			
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
				e.printStackTrace();
				throw new SemossPixelException("Error in executing the param query for the insight");
			} finally {
				if(it != null) {
					it.cleanUp();
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

		return this.qs;
	}
	
}
