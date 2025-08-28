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

import java.util.ArrayList;
import java.util.List;
import prerna.masterdatabase.AddToMasterDB;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class DeleteMetaTagsReactor extends AbstractMetaDBReactor {

  public DeleteMetaTagsReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), VALUES};
  }

  @Override
  public NounMetadata execute() {
    String engineId = getEngineId();
    engineId = MasterDatabaseUtility.testDatabaseIdIfAlias(engineId);
    String concept = getConcept();
    List<String> valuesToDelete = getValues();
    String oldTagList = MasterDatabaseUtility.getMetadataValue(engineId, concept, Constants.TAG);
    ArrayList<String> tags = new ArrayList<String>();
    // organize existing tags into list
    for (String tag : oldTagList.split(VALUE_DELIMITER)) {
      if (tag.length() > 0) {
        tags.add(tag);
      }
    }
    // delete tags from list
    for (String deleteTag : valuesToDelete) {
      int indexToRemove = tags.indexOf(deleteTag);
      if (indexToRemove >= 0) {
        tags.remove(indexToRemove);
      }
    }
    // update list
    String newTagList = "";
    for (String newTag : tags) {
      newTagList += newTag + VALUE_DELIMITER;
    }
    AddToMasterDB master = new AddToMasterDB();
    boolean success = master.addMetadata(engineId, concept, Constants.TAG, newTagList);
    return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
  }
}
