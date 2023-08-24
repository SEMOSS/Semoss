package prerna.sablecc2.reactor.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class MyProjectsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(MyProjectsReactor.class);

	public MyProjectsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.ONLY_FAVORITES.getKey(), ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(),
				ReactorKeysEnum.PERMISSION_FILTERS.getKey(), ReactorKeysEnum.NO_META.getKey(), ReactorKeysEnum.ONLY_PORTALS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// add creator, upvotes, total views
		// sort by name, date created, views, upvotes, trending
		
		organizeKeys();
		String limit = this.keyValue.get( ReactorKeysEnum.LIMIT.getKey());
		String offset = this.keyValue.get(ReactorKeysEnum.OFFSET.getKey());
		Boolean favoritesOnly = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.ONLY_FAVORITES.getKey())+"");
		Boolean noMeta = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.NO_META.getKey())+"");
		Boolean portalsOnly = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.ONLY_PORTALS.getKey())+"");
		List<Integer> permissionFilters = getPermissionFilters();
		Map<String, Object> projectMetadataFilter = getMetaMap();

		List<Map<String, Object>> projectInfo = SecurityProjectUtils.getUserProjectList(this.insight.getUser(), favoritesOnly, portalsOnly,
				projectMetadataFilter, permissionFilters, limit, offset);
		if(!favoritesOnly) {
			this.insight.getUser().setProjects(projectInfo);
		}

		if(!projectInfo.isEmpty() && !noMeta) {
			Map<String, Integer> index = new HashMap<>(projectInfo.size());
			int size = projectInfo.size();
			// now we want to add most executed insights
			for(int i = 0; i < size; i++) {
				Map<String, Object> project = projectInfo.get(i);
				String projectId = project.get("project_id").toString();
				// keep list of project ids to get the index
				index.put(projectId, Integer.valueOf(i));
			}
	
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = SecurityProjectUtils.getProjectMetadataWrapper(index.keySet(), getMetaKeys());
				while(wrapper.hasNext()) {
					Object[] data = wrapper.next().getValues();
					String projectId = (String) data[0];
	
					String metaKey = (String) data[1];
					String metaValue = (String) data[2];
					if(metaValue == null) {
						continue;
					}
	
					int indexToFind = index.get(projectId);
					Map<String, Object> res = projectInfo.get(indexToFind);
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
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}
	
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
	private List<Integer> getPermissionFilters() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PERMISSION_FILTERS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllNumericColumnsAsInteger();
		}
		
		return null;
	}
}
