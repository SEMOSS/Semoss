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
package prerna.reactor.database.metaeditor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.engine.api.IDatabaseEngine;
import prerna.masterdatabase.utility.MetamodelVertex;
import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetOwlMetamodelReactor extends AbstractMetaEditorReactor {

  public GetOwlMetamodelReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String databaseId = this.keyValue.get(this.keysToGet[0]);
    // we may have the alias
    databaseId = testDatabaseId(databaseId, false);
    IDatabaseEngine database = Utility.getDatabase(databaseId);
    Map<String, Object[]> metamodelObject = database.getMetamodel();
    Object[] nodes = metamodelObject.get("nodes");
    Object[] relationships = metamodelObject.get("edges");
    Map<String, Collection<String>> concepts = new HashMap<String, Collection<String>>();

    for (Object node : nodes) {
      MetamodelVertex v = (MetamodelVertex) node;
      concepts.put(v.getConceptualName(), new ArrayList<String>(v.getPropSet()));
    }
    List<Map<String, String>> rels = new ArrayList<>();

    for (Object relation : relationships) {
      rels.add((Map<String, String>) relation);
    }

    HashMap<String, Object> returnMap = new HashMap<String, Object>();
    returnMap.put(Constants.NODE_PROP, nodes);
    returnMap.put(Constants.RELATION_PROP, relationships);
    Map<String, Map<String, Double>> positions =
        GenerateMetamodelLayout.generateOWLMetamodelLayout(concepts, rels);
    returnMap.put(Constants.POSITION_PROP, positions);

    return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
  }
}
