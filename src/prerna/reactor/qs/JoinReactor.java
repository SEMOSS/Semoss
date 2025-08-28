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
package prerna.reactor.qs;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;

public class JoinReactor extends AbstractQueryStructReactor {

  public JoinReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.JOINS.getKey()};
  }

  @Override
  protected AbstractQueryStruct createQueryStruct() {
    GenRowStruct joins = getNounStore().getNoun(NounStore.all);
    for (int i = 0; i < joins.size(); i++) {
      if (joins.get(i) instanceof Join) {
        Join join = (Join) joins.get(i);
        qs.addRelation(
            join.getLColumn(),
            join.getRColumn(),
            join.getJoinType(),
            join.getComparator(),
            join.getJoinRelName());
      }
    }

    return qs;
  }
}
