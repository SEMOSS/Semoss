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
package prerna.masterdatabase.utility;

import java.util.List;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.Utility;

public class MetadataUtility {

  private MetadataUtility() {}

  public static boolean ignoreConceptData(IDatabaseEngine.DATABASE_TYPE type) {
    return type == IDatabaseEngine.DATABASE_TYPE.RDBMS
        || type == IDatabaseEngine.DATABASE_TYPE.R
        || type == IDatabaseEngine.DATABASE_TYPE.IMPALA
    //				|| type == IDatabase.ENGINE_TYPE.JMES_API
    ;
  }

  public static boolean ignoreConceptData(String engineId) {
    String eType = MasterDatabaseUtility.getDatabaseTypeForId(engineId);
    if (eType.startsWith("TYPE:")) {
      eType = eType.replace("TYPE:", "");
    }
    if (eType.equals("RDF")) {
      eType = "SESAME";
    }
    return ignoreConceptData(IDatabaseEngine.DATABASE_TYPE.valueOf(eType));
  }

  /**
   * Check if a given property name is already present in an existing concept
   *
   * @param engine
   * @param conceptPhysicalUri
   * @param propertyName
   * @return
   */
  public static boolean propertyExistsForConcept(
      IDatabaseEngine engine, String conceptPhysicalUri, String propertyName) {
    List<String> properties = engine.getPropertyUris4PhysicalUri(conceptPhysicalUri);
    for (String prop : properties) {
      if (propertyName.equalsIgnoreCase(Utility.getInstanceName(prop))) {
        return true;
      }
    }

    return false;
  }
}
