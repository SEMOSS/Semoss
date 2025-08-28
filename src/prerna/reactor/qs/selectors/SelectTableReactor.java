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
package prerna.reactor.qs.selectors;

import java.util.List;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.ReactorKeysEnum;

public class SelectTableReactor extends AbstractQueryStructReactor {

  public SelectTableReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.TABLE.getKey()};
  }

  @Override
  protected AbstractQueryStruct createQueryStruct() {
    organizeKeys();
    // must have used Database reactor before hand
    // so we know this must be the id
    String databaseId = qs.getEngineId();
    if (databaseId == null) {
      throw new IllegalArgumentException(
          "Must define the database using Database(<input id here>) prior to SelectTable");
    }
    String table = this.keyValue.get(ReactorKeysEnum.TABLE.getKey());

    List<String> selectors = MasterDatabaseUtility.getConceptPixelSelectors(table, databaseId);
    for (int i = 0; i < selectors.size(); i++) {
      QueryColumnSelector qsSelector = new QueryColumnSelector(selectors.get(i));
      qs.addSelector(qsSelector);
    }

    return qs;
  }
}
