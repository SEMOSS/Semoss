/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.model.inferencetracking.reactors.workspaces;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.algorithm.api.SemossDataType;
import prerna.auth.AuthProvider;
import prerna.auth.User;
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

public class GetWorkspaceRoomsReactor extends AbstractReactor {

  private static final Map<String, SemossDataType> TYPES_FOR_SUBQUERY_COLUMNS = new HashMap<>();

  static {
    TYPES_FOR_SUBQUERY_COLUMNS.put("room_id", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("room_name", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("room_context", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("model_id", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("workspace_id", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("date_created", SemossDataType.STRING);
    TYPES_FOR_SUBQUERY_COLUMNS.put("date_updated", SemossDataType.STRING);
  }

  public GetWorkspaceRoomsReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.WORKSPACE_ID.getKey(),
          ReactorKeysEnum.LIMIT.getKey(),
          ReactorKeysEnum.OFFSET.getKey(),
          ReactorKeysEnum.FILTERS.getKey(),
          ReactorKeysEnum.SORT.getKey()
        };
    this.keyRequired = new int[] {1, 0, 0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

    User user = this.insight.getUser();
    long limit = getLimit();
    long offset = getOffset();
    GenRowFilters filters = getFilters();
    List<IQuerySort> sorts = getSorts();

    Map<String, Object> current = ModelInferenceLogsUtils.getWorkspaceEntry(workspaceId);
    if (current == null) {
      throw new IllegalArgumentException("Workspace not found");
    }
    String currentOwner = (String) current.get("owner");

    Object currentlySharingEnabled = current.get("sharing_enabled");
    Boolean currentlyShared = (Boolean) currentlySharingEnabled;

    boolean hasPermission = false;
    if (currentOwner != null) {
      for (AuthProvider provider : user.getLogins()) {
        if (currentOwner.equalsIgnoreCase(user.getAccessToken(provider).getId())) {
          hasPermission = true;
          break;
        }
      }
    }
    if (!hasPermission
        && (Boolean.TRUE != currentlyShared
            || !ModelInferenceLogsUtils.isWorkspaceSharedWithUser(workspaceId, user))) {
      throw new IllegalArgumentException("User unauthorized to perform this operation");
    }

    Map<String, Object> rooms =
        ModelInferenceLogsUtils.getWorkspaceRoomsForUser(
            workspaceId, user, limit, offset, filters, sorts);
    return new NounMetadata(rooms, PixelDataType.MAP);
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
