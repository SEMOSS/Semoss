package prerna.sablecc2.reactor.project;

import java.util.ArrayList;
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

	public MyProjectsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey(),
				ReactorKeysEnum.ONLY_FAVORITES.getKey(), ReactorKeysEnum.META_KEYS.getKey(), ReactorKeysEnum.META_FILTERS.getKey(),
				ReactorKeysEnum.NO_META.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// add creator, upvotes, total views
		// sort by name, date created, views, upvotes, trending
		
		organizeKeys();
		String limit = this.keyValue.get(this.keysToGet[0]);
		String offset = this.keyValue.get(this.keysToGet[1]);
		Boolean favoritesOnly = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]));
		Boolean noMeta = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.NO_META.getKey()));
		
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
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					wrapper.cleanUp();
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
}
