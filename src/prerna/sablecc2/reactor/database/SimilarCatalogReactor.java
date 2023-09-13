package prerna.sablecc2.reactor.database;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.usertracking.UserCatalogVoteUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SimilarCatalogReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SimilarCatalogReactor.class);
	
	public SimilarCatalogReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database cannot be viewed by user.");
		}
		
		String dbAlias = MasterDatabaseUtility.getDatabaseAliasForId(databaseId);

		List<String> engineTypes = new ArrayList<>();
		List<Map<String, Object>> dbInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(), engineTypes, null, false, null, null, null, null, "0");
		List<String> dbIds = dbInfo.stream().map(s-> s.get("database_id").toString()).collect(Collectors.toList());
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
		
		List<Object> result = pt.getList("result");
		if (result == null || result.size() == 0) {
			return new NounMetadata("No Catalogs found for search term", PixelDataType.CONST_STRING);
		}
		
		Set<String> ids = new HashSet<>();
		for (Object res : result) {
			String alias = res.toString();
			List<String> aliasedIds = MasterDatabaseUtility.getDatabaseIdsForAlias(alias);
			if (aliasedIds == null || aliasedIds.size() == 0) {
				continue;
			}
			ids.addAll(aliasedIds);
		}
		
		List<Map<String, Object>> toReturn = new ArrayList<>();
		for (Map<String, Object> x : dbInfo) {
			if (ids.contains(x.get("database_id").toString())) {
				toReturn.add(x);
			}
		}
		
		Map<String, Integer> index = new HashMap<>();
		for (int i = 0; i < toReturn.size(); i++) {
			index.put(toReturn.get(i).get("database_id").toString(), i);
		}
		
		List<String> toReturnDBIds = toReturn.stream()
				.map(s -> s.get("database_id").toString())
				.collect(Collectors.toList());
		
		addEngineMetadata(toReturn, toReturnDBIds, index);

		if (Utility.isUserTrackingEnabled()) {
			addUserTrackingData(toReturn, toReturnDBIds, index);
		}
		
		return new NounMetadata(toReturn, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	/**
	 * 	
	 * @param toReturn
	 * @param toReturnDBIds
	 * @param index
	 */
	private void addUserTrackingData(List<Map<String, Object>> toReturn, List<String> toReturnDBIds, Map<String, Integer> index) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = UserCatalogVoteUtils.getAllVotesWrapper(toReturnDBIds);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String databaseId = (String) data[0];
				int upvotes = ((Number) data[1]).intValue();

				int indexToFind = index.get(databaseId);
				Map<String, Object> res = toReturn.get(indexToFind);
				res.put("upvotes", upvotes);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper!=null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * 
	 * @param dbInfo
	 * @param toReturnDBIds
	 * @param index
	 * @return
	 */
	private IRawSelectWrapper addEngineMetadata(List<Map<String, Object>> dbInfo, List<String> toReturnDBIds, Map<String, Integer> index) {
		IRawSelectWrapper wrapper = null;
		try {
			List<String> keys = new ArrayList<>();
			keys.add("markdown");
			keys.add("description");
			keys.add("tag");
			keys.add("domain");
			
			wrapper = SecurityEngineUtils.getEngineMetadataWrapper(toReturnDBIds, keys, true);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				String databaseId = (String) data[0];

				String metaKey = (String) data[1];
				String metaValue = (String) data[2];
				if(metaValue == null) {
					continue;
				}

				int indexToFind = index.get(databaseId);
				Map<String, Object> res = dbInfo.get(indexToFind);
				// whatever it is, if it is single send a single value, if it is multi send as array
				if(res.containsKey(metaKey)) {
					Object obj = res.get(metaKey);
					if(obj instanceof List) {
						((List) obj).add(metaValue);
					} else {
						List<Object> newList = new ArrayList<>();
						newList.add(obj);
						newList.add(metaValue);
						res.put(metaKey, newList);
					}
				} else {
					res.put(metaKey, metaValue);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper!=null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return wrapper;
	}

}
