package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.transform.QSFilterToTypedConverter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListWorkspacesReactor extends AbstractReactor {

  private static final Map<String, SemossDataType> TYPES_FOR_SUBQUERY_COLUMNS = new HashMap<>();
  private Map<String, Map<String, Object>> sharedWorkspaceMetadata = new HashMap<>();

  static {
    TYPES_FOR_SUBQUERY_COLUMNS.put("workspace_id", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("name", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("description", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("system_prompt", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("owner", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("sharing_enabled", SemossDataType.BOOLEAN);
    TYPES_FOR_SUBQUERY_COLUMNS.put("date_created", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("date_updated", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("is_creator", SemossDataType.BOOLEAN);
    TYPES_FOR_SUBQUERY_COLUMNS.put("is_active", SemossDataType.BOOLEAN);
  }

  public ListWorkspacesReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.LIMIT.getKey(),
          ReactorKeysEnum.OFFSET.getKey(),
          ReactorKeysEnum.FILTERS.getKey(),
          ReactorKeysEnum.SORT.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    User user = this.insight.getUser();
    long limit = getLimit();
    long offset = getOffset();
    GenRowFilters filters = getFilters();
    List<IQuerySort> sorts = getSorts();
    Set<String> sharedWorkspaceIds = getSharedWorkspaceIds(user);

    Map<String, Object> workspaces =
        ModelInferenceLogsUtils.getWorkspaceEntriesForUser(
            user, limit, offset, filters, sorts, sharedWorkspaceIds);
    if (workspaces == null) {
      return getError("There was a problem retrieving workspaces");
    }
    
    try {
    	List<Map<String, Object>> workspaceEntries = (List<Map<String, Object>>) workspaces.get("workspaces");
        
        for (Map<String, Object> workspace : workspaceEntries) {
        	String workspaceKey = (String) workspace.get("workspace_id");
        	if (sharedWorkspaceMetadata.containsKey(workspaceKey)) {
        		Map<String, Object> workspaceMeta = sharedWorkspaceMetadata.get(workspaceKey);
        		workspace.put("permission", (String) workspaceMeta.get("permission"));
        		workspace.put("number_collaborators", (long) workspaceMeta.get("number_collaborators"));
        	} else {
        		workspace.put("permission", AccessPermissionEnum.OWNER.getPermission());
        		workspace.put("number_collaborators", 1);
        	}
        }
    } catch (Exception e) {
    	return getError("There was a problem retrieving workspaces");
    }
    
    return new NounMetadata(workspaces, PixelDataType.MAP);
  }

  private Set<String> getSharedWorkspaceIds(User user) {
    Map<String, Object> projectMetadataFilter = new HashMap<>();
    projectMetadataFilter.put("tag", ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
    List<Map<String, Object>> projectInfo =
        SecurityProjectUtils.getUserProjectList(
            user, null, null, false, false, projectMetadataFilter, null, null, null, null);

    Set<String> sharedWorkspaceIds = new HashSet<>();
    for (Map<String, Object> project : projectInfo) {
    	String projectId = (String) project.get("project_id");
    	Integer permission = (Integer) project.get("user_permission");
    	
    	sharedWorkspaceIds.add(projectId);
    	try {
            long userCount = SecurityProjectUtils.getProjectUsersCount(user, projectId, null, null);
            Map<String, Object> meta = new HashMap<>();
            meta.put("number_collaborators", userCount);
            meta.put("permission", AccessPermissionEnum.getPermissionValueById(permission));
            sharedWorkspaceMetadata.put(projectId, meta);
          } catch (IllegalAccessException e) {
            e.printStackTrace();
          }
    }
    return sharedWorkspaceIds;
  }

  private GenRowFilters getFilters() {
    GenRowFilters grf = new GenRowFilters();
    GenRowStruct grs = this.getNounStore().getGenRowStruct(ReactorKeysEnum.FILTERS.getKey());
    if (grs != null && !grs.isEmpty()) {
      int size = grs.size();
      for (int i = 0; i < size; i++) {
        Object val = grs.get(i);
        SelectQueryStruct qs = (SelectQueryStruct) val;
        if (qs != null) {
          grf.merge(qs.getCombinedFilters());
        }
      }
    }

    if (grf != null && !grf.isEmpty()) {
      List<IQueryFilter> filters =
          QSFilterToTypedConverter.convertFilters(
              grf.getFilters(), "subquery", TYPES_FOR_SUBQUERY_COLUMNS);
      return new GenRowFilters(filters);
    }

    return null;
  }

  private List<IQuerySort> getSorts() {
    GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.SORT.getKey());
    if (inputsGRS != null && !inputsGRS.isEmpty()) {
      NounMetadata sortNoun = inputsGRS.getNoun(0);
      SelectQueryStruct qs = (SelectQueryStruct) sortNoun.getValue();
      List<IQuerySort> orderBy = qs.getOrderBy();
      return orderBy;
    }
    return null;
  }

  private long getLimit() {
    GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.LIMIT.getKey());
    if (inputsGRS != null && !inputsGRS.isEmpty()) {
      NounMetadata limitNoun = inputsGRS.getNoun(0);
      return ((Number) limitNoun.getValue()).longValue();
    }
    return -1;
  }

  private long getOffset() {
    GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.OFFSET.getKey());
    if (inputsGRS != null && !inputsGRS.isEmpty()) {
      NounMetadata offsetNoun = inputsGRS.getNoun(0);
      return ((Number) offsetNoun.getValue()).longValue();
    }
    return -1;
  }
}
