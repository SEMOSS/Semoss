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
package prerna.query.querystruct.joins;

import com.google.gson.TypeAdapter;
import prerna.util.gson.BasicRelationshipAdapter;
import prerna.util.gson.SubqueryRelationshipAdapter;

public interface IRelation {

  enum RELATION_TYPE {
    BASIC,
    SUBQUERY
  }

  RELATION_TYPE getRelationType();

  ////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////

  /*
   *
   * Methods around serialization
   *
   */

  static TypeAdapter getAdapterForRelation(RELATION_TYPE type) {
    if (type == RELATION_TYPE.BASIC) {
      return new BasicRelationshipAdapter();
    } else if (type == RELATION_TYPE.SUBQUERY) {
      return new SubqueryRelationshipAdapter();
    }

    return null;
  }

  /**
   * Convert string to SELECTOR_TYPE
   *
   * @param s
   * @return
   */
  static RELATION_TYPE convertStringToRelationType(String s) {
    if (s.equals(RELATION_TYPE.BASIC.toString())) {
      return RELATION_TYPE.BASIC;
    } else if (s.equals(RELATION_TYPE.SUBQUERY.toString())) {
      return RELATION_TYPE.SUBQUERY;
    }

    return null;
  }
}
