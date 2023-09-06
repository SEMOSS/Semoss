package prerna.sablecc2.reactor.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.ds.py.PyTranslator;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SimilarCatalogReactor extends AbstractReactor {
	
	public SimilarCatalogReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		String dbAlias = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);
		
		List<String> dbIds = MasterDatabaseUtility.getAllDatabaseIds();
		String metadatas = "metadatas = {";
		for (String s : dbIds) {
			String alias = MasterDatabaseUtility.getDatabaseAliasForId(s);
			List<Object[]> tac = MasterDatabaseUtility.getAllTablesAndColumns(s);
			String keywords = tac.stream().map(t -> (String) t[1]).map(t -> "'" + t + "'")
					.reduce((a, b) -> a + "," + b).orElse("");
			alias = "'" + alias + "'";
			if (!keywords.isEmpty()) {
				keywords = alias + "," + keywords;
			} else {
				keywords = alias;
			}
			metadatas = metadatas + alias + ":" + "(" + keywords + "),";
		}
		metadatas = metadatas.substring(0, metadatas.length() - 1);
		metadatas = metadatas + "}";
		dbAlias = "alias = \"" + dbAlias + "\"";
		
		String path = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + File.separator
				+ Constants.PY_BASE_FOLDER + File.separator + "similarcatalog.py";
		Path p = Paths.get(path);
		if (Files.notExists(p)) {
			throw new IllegalArgumentException("Python file does not exist.");
		}
		
		String[] script;
		try {
			script = Files.readAllLines(p).toArray(new String[] {});
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not load python file", e);
		}
		
		PyTranslator pt = this.insight.getPyTranslator();
		pt.runEmptyPy(metadatas);
		pt.runEmptyPy(dbAlias);
		pt.runEmptyPy(script);
		
		Boolean hasResults = pt.getBoolean("has_results");
		if (hasResults == null || !hasResults) {
			return new NounMetadata("No Catalogs found for search term", PixelDataType.CONST_STRING);
		}

		List<Map<String, String>> dbs = new ArrayList<>();

		List<Object> result = pt.getList("result");
		for (Object res : result) {
			String alias = res.toString();
			List<String> ids = MasterDatabaseUtility.getDatabaseIdsForAlias(alias);
			if (ids == null || ids.size() == 0) {
				continue;
			}
			Map<String, String> dbMap = new HashMap<>();
			dbMap.put("database_id", ids.get(0));
			dbMap.put("database_name", alias);
			dbs.add(dbMap);
		}
		
		return new NounMetadata(dbs, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
