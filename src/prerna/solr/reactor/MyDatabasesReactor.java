package prerna.solr.reactor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.usertracking.UserCatalogVoteUtils;
import prerna.util.Constants;

public class MyDatabasesReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(MyDatabasesReactor.class);

	public MyDatabasesReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILTER_WORD.getKey(), 
				ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.ONLY_FAVORITES.getKey(), ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.PERMISSION_FILTERS.getKey(),
				ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(),
				ReactorKeysEnum.NO_META.getKey(), ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey()
			};
	}

	@Override
	public NounMetadata execute() {
		// add creator, upvotes, total views
		// sort by name, date created, views, upvotes, trending
		
		organizeKeys();
		
		List<String> engineTypes = new ArrayList<>();
		// keep empty cause security db is likely blank for this
//		engineTypes.add(IEngine.CATALOG_TYPE);
		
		String searchTerm = this.keyValue.get(this.keysToGet[0]);
		String limit = this.keyValue.get(this.keysToGet[1]);
		String offset = this.keyValue.get(this.keysToGet[2]);
		Boolean favoritesOnly = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]));
		List<String> databaseFilter = getDatabaseFilters();
		Boolean noMeta = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.NO_META.getKey()));
		List<Integer> permissionFilters = getPermissionFilters();
		Boolean includeUserT = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.INCLUDE_USERTRACKING_KEY.getKey()));
		
		Map<String, Object> engineMetadataFilter = getMetaMap();
		List<Map<String, Object>> dbInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(), engineTypes, databaseFilter, favoritesOnly, engineMetadataFilter, permissionFilters, searchTerm, limit, offset);
		if(!favoritesOnly) {
			this.insight.getUser().setEngines(dbInfo);
		}

		if(!dbInfo.isEmpty() && (!noMeta || includeUserT)) {
			Map<String, Integer> index = new HashMap<>(dbInfo.size());
			int size = dbInfo.size();
			for(int i = 0; i < size; i++) {
				Map<String, Object> database = dbInfo.get(i);
				String databaseId = database.get("database_id").toString();
				// keep list of database ids to get the index
				index.put(databaseId, Integer.valueOf(i));
			}
			
			if(!noMeta) {
				IRawSelectWrapper wrapper = null;
				try {
					wrapper = SecurityEngineUtils.getEngineMetadataWrapper(index.keySet(), getMetaKeys(), true);
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
			} 
			if(includeUserT) {
				IRawSelectWrapper wrapper = null;
				try {
					wrapper = UserCatalogVoteUtils.getAllVotesWrapper(index.keySet());
					while(wrapper.hasNext()) {
						Object[] data = wrapper.next().getValues();
						String databaseId = (String) data[0];
						int upvotes = ((Number) data[1]).intValue();
		
						int indexToFind = index.get(databaseId);
						Map<String, Object> res = dbInfo.get(indexToFind);
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
		}
		
		return new NounMetadata(dbInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}
	
	/**
	 * 
	 * @return
	 */
	private List<String> getDatabaseFilters() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.DATABASE.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	private List<Integer> getPermissionFilters() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PERMISSION_FILTERS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllNumericColumnsAsInteger();
		}
		
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.META_KEYS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}
	
	/**
	 * 
	 * @return
	 */
	private Map<String, Object> getMetaMap() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.META_FILTERS.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}
		return null;
	}

	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.SORT.getKey())) {
			return "The sort is a string value containing either 'name' or 'date' for how to sort";
		} else if(key.equals(ReactorKeysEnum.DATABASE.getKey())) {
			return "This is an optional database filter";
		}
		return super.getDescriptionForKey(key);
	}
}
