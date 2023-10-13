package prerna.reactor.database;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class DatabaseColumnUniqueReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DatabaseColumnUniqueReactor.class);

	public DatabaseColumnUniqueReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String engineId = getApp();
		List<String> columnNames = getColumns();
		
		// we may have the alias
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have access to database");
		}
		
		IDatabaseEngine engine = Utility.getDatabase(engineId);
		
		long nRow = 0;
		long uniqueNRow = 0;
		
		// query to get the row count
		// and unique row count
		{
			SelectQueryStruct qs1 = new SelectQueryStruct();
			qs1.setDistinct(false);
			for(String columnName : columnNames) {
				QueryColumnSelector columnSelector = new QueryColumnSelector();
				if(columnName.contains("__")) {
					String[] split = columnName.split("__");
					columnSelector.setTable(split[0]);
					columnSelector.setColumn(split[1]);
				} else {
					columnSelector.setTable(columnName);
					columnSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
				}
				qs1.addSelector(columnSelector);
			}
			
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(engine, qs1);
				nRow = it.getNumRows();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(e.getMessage());
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}

		{
			SelectQueryStruct qs2 = new SelectQueryStruct();
			for(String columnName : columnNames) {
				QueryColumnSelector columnSelector = new QueryColumnSelector();
				if(columnName.contains("__")) {
					String[] split = columnName.split("__");
					columnSelector.setTable(split[0]);
					columnSelector.setColumn(split[1]);
				} else {
					columnSelector.setTable(columnName);
					columnSelector.setColumn(SelectQueryStruct.PRIM_KEY_PLACEHOLDER);
				}
				qs2.addSelector(columnSelector);
			}
			
			IRawSelectWrapper it = null;
			try {
				it = WrapperManager.getInstance().getRawWrapper(engine, qs2);
				uniqueNRow = it.getNumRows();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(e.getMessage());
			} finally {
				if(it != null) {
					try {
						it.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// if they are not equal, we have duplicates!
		boolean isUnique = (long) nRow == (long) uniqueNRow;
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("isUnique", isUnique);
		retMap.put("numRow", nRow);
		retMap.put("uniqueRow", uniqueNRow);

		return new NounMetadata(retMap, PixelDataType.MAP);
	}

	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////

	private String getApp() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return (String) grs.get(0);
		}
		
		return (String) this.curRow.get(0);
	}

	private List<String> getColumns() {
		List<String> cols = new Vector<String>();

		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				cols.add(grs.get(i).toString());
			}
			return cols;
		}
		
		// start at index 1 since 0 must be for app
		List<String> values = this.curRow.getAllStrValues();
		for(int i = 1; i < values.size(); i++) {
			cols.add(values.get(i));
		}
		
		return cols;
	}
}