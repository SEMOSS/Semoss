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
package prerna.reactor.masterdatabase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetDatabaseListReactor extends AbstractReactor {

  public GetDatabaseListReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.LIMIT.getKey(), ReactorKeysEnum.OFFSET.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String strLimit = this.keyValue.get(this.keysToGet[0]);
    String strOffset = this.keyValue.get(this.keysToGet[1]);

    Integer limit = null;
    Integer offset = null;
    if (strLimit != null && !(strLimit = strLimit.trim()).isEmpty()) {
      try {
        limit = Integer.parseInt(strLimit);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Could not parse limit value as integer. Input was: " + strLimit);
      }
    }
    if (strOffset != null && !(strOffset = strOffset.trim()).isEmpty()) {
      try {
        offset = Integer.parseInt(strOffset);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Could not parse offset value as integer. Input was: " + strOffset);
      }
    }

    List<String> engineTypeFilter = Arrays.asList(IEngine.CATALOG_TYPE.DATABASE.name());
    List<Map<String, Object>> retList =
        SecurityEngineUtils.getUserEngineList(
            this.insight.getUser(), engineTypeFilter, limit, offset);
    return new NounMetadata(
        retList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_LIST);
  }
}
