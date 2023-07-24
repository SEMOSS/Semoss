package prerna.sablecc2.reactor.utils;

import java.util.List;

import prerna.engine.api.IDatabase;
import prerna.engine.api.IDatabase.ENGINE_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class CheckRecommendOptimizationReactor extends AbstractReactor {

	public CheckRecommendOptimizationReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);

		IDatabase database = Utility.getEngine(databaseId);
		ENGINE_TYPE type = database.getEngineType();
		RDFFileSesameEngine owlEngine = null;
		if (type.equals(ENGINE_TYPE.RDBMS)) {
			RDBMSNativeEngine eng = (RDBMSNativeEngine) database;
			owlEngine = eng.getBaseDataEngine();
		} else if (type.equals(ENGINE_TYPE.TINKER)) {
			TinkerEngine eng = (TinkerEngine) database;
			owlEngine = eng.getBaseDataEngine();
		} else if (type.equals(ENGINE_TYPE.SESAME)) {
			BigDataEngine eng = (BigDataEngine) database;
			owlEngine = eng.getBaseDataEngine();
		} else if (type.equals(ENGINE_TYPE.JENA)) {
			RDFFileSesameEngine eng = (RDFFileSesameEngine) database;
			owlEngine = eng.getBaseDataEngine();
		}

		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(databaseId);
		for (Object[] tableCol : allTableCols) {
			if (tableCol.length == 4) {
				String table = tableCol[0] + "";
				String col = tableCol[1] + "";
				String dataType = tableCol[2] + "";
				// only care about strings
				if (dataType != null && dataType.equals("STRING")) {
					String queryCol = col;
					String uniqueValQuery = "SELECT DISTINCT ?concept ?unique WHERE "
							+ "{ BIND(<http://semoss.org/ontologies/Concept/" + queryCol + "/" + table
							+ "> AS ?concept)"
							+ "{?concept <http://semoss.org/ontologies/Relation/Contains/UNIQUE> ?unique}}";
					IRawSelectWrapper it = null;
					try {
						it = WrapperManager.getInstance().getRawWrapper(owlEngine, uniqueValQuery);
						if (it.hasNext()) {
							// it had a unique value so we assume they're all there
							return new NounMetadata(true, PixelDataType.BOOLEAN);
						} else {
							// the first string value didn't have a unique count so
							// we assume none do
							return new NounMetadata(false, PixelDataType.BOOLEAN);
						}
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if(it != null) {
							it.cleanUp();
						}
					}
					return new NounMetadata(false, PixelDataType.BOOLEAN);
				}
			}
		}
		// none were strings so no need to get uniques
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
