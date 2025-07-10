package prerna.engine.impl.model.workspace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import prerna.algorithm.api.SemossDataType;
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
    return new NounMetadata(workspaces, PixelDataType.MAP);
  }

  private Set<String> getSharedWorkspaceIds(User user) {
    Map<String, Object> projectMetadataFilter = new HashMap<>();
    projectMetadataFilter.put("tag", ModelInferenceLogsUtils.WORKSPACE_PROJECT_TAG);
    List<Map<String, Object>> projectInfo =
        SecurityProjectUtils.getUserProjectList(
            user, null, null, false, false, projectMetadataFilter, null, null, null, null);
    Set<String> sharedWorkspaceIds =
        projectInfo.stream()
            .map(info -> (String) info.get("project_id"))
            .collect(Collectors.toSet());
    return sharedWorkspaceIds;
  }

  private GenRowFilters getFilters() {
    GenRowFilters grf = new GenRowFilters();
    GenRowStruct grs = this.getNounStore().getNoun(ReactorKeysEnum.FILTERS.getKey());
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
    GenRowStruct inputsGRS = this.store.getNoun(ReactorKeysEnum.SORT.getKey());
    if (inputsGRS != null && !inputsGRS.isEmpty()) {
      NounMetadata sortNoun = inputsGRS.getNoun(0);
      SelectQueryStruct qs = (SelectQueryStruct) sortNoun.getValue();
      List<IQuerySort> orderBy = qs.getOrderBy();
      return orderBy;
    }
    return null;
  }

  private long getLimit() {
    GenRowStruct inputsGRS = this.store.getNoun(ReactorKeysEnum.LIMIT.getKey());
    if (inputsGRS != null && !inputsGRS.isEmpty()) {
      NounMetadata limitNoun = inputsGRS.getNoun(0);
      return ((Number) limitNoun.getValue()).longValue();
    }
    return -1;
  }

  private long getOffset() {
    GenRowStruct inputsGRS = this.store.getNoun(ReactorKeysEnum.OFFSET.getKey());
    if (inputsGRS != null && !inputsGRS.isEmpty()) {
      NounMetadata offsetNoun = inputsGRS.getNoun(0);
      return ((Number) offsetNoun.getValue()).longValue();
    }
    return -1;
  }
}
