package prerna.sablecc2.reactor.project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
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
	
	private static final Logger logger = LogManager.getLogger(MyProjectsReactor.class);

	private static final String META_KEYS = "metaKeys";
	private static final String META_FILTERS = "metaFilters";

	public MyProjectsReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.ONLY_FAVORITES.getKey(), META_KEYS, META_FILTERS};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String limit = this.keyValue.get(this.keysToGet[0]);
		String offset = this.keyValue.get(this.keysToGet[1]);
		Boolean favoritesOnly = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]));
		
		List<Map<String, Object>> projectInfo = new Vector<>();

		if(AbstractSecurityUtils.securityEnabled()) {
			Map<String, Object> projectMetadataFilter = getMetaMap();
			projectInfo = SecurityProjectUtils.getUserProjectList(this.insight.getUser(), favoritesOnly, 
					projectMetadataFilter, limit, offset);
			if(!favoritesOnly) {
				this.insight.getUser().setProjects(projectInfo);
			}
		} else {
			projectInfo = SecurityProjectUtils.getAllProjectList(null, limit, offset);
		}

		int size = projectInfo.size();
		Map<String, Integer> index = new HashMap<>(projectInfo.size());
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
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		// need to go through and sort the values
		Collections.sort(projectInfo, new Comparator<Map<String, Object>>() {

			// we want descending - not ascending
			// so values are swapped
			@Override
			public int compare(Map<String, Object> o1, Map<String, Object> o2) {
				String name1 = (String) o1.get("low_project_name");
				String name2 = (String) o2.get("low_project_name");
				
				return name1.compareTo(name2);
			}
		});

		return new NounMetadata(projectInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}
	
	private Map<String, Object> getMetaMap() {
		GenRowStruct mapGrs = this.store.getNoun(META_FILTERS);
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
		GenRowStruct grs = this.store.getNoun(META_KEYS);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(META_KEYS)) {
			return "List of the metadata keys to return with each project";
		} else if(key.equals(META_FILTERS)) {
			return "Map containing key-value pairs for filters to apply on the project metadata";
		}
		return super.getDescriptionForKey(key);
	}
}
