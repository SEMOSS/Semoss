package prerna.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.parsers.SqlTranslator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class QueryTranslatorReactor extends AbstractReactor {
	public QueryTranslatorReactor() {
		this.keysToGet = new String[] { "query", "sourceDB", "targetDB" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		query = Utility.decodeURIComponent(query);
		System.out.println(query);
		String sourceDbId = this.keyValue.get(this.keysToGet[1]);
		String targetDbId = this.keyValue.get(this.keysToGet[2]);
		sourceDbId = MasterDatabaseUtility.testDatabaseIdIfAlias(sourceDbId);
		targetDbId = MasterDatabaseUtility.testDatabaseIdIfAlias(targetDbId);
		// get physical to physical translation from sourceDB to targetDB
		Map<String, List<String>> translation = MasterDatabaseUtility.databaseTranslator(sourceDbId, targetDbId);
		// generate translated queries
		SqlTranslator translator = new SqlTranslator(translation);
		Set<String> queries = null;
		try {
			queries = translator.processQuery(query);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new NounMetadata(queries, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
}
