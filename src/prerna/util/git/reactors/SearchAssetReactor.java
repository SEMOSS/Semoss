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
package prerna.util.git.reactors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitAssetUtils;

public class SearchAssetReactor extends AbstractReactor {

  public SearchAssetReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.SEARCH.getKey(),
          ReactorKeysEnum.FILE_PATH.getKey(),
          ReactorKeysEnum.SPACE.getKey()
        };
    this.keyRequired = new int[] {1, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String space = this.keyValue.get(this.keysToGet[2]);
    String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false);

    // get search term
    String search = keyValue.get(keysToGet[0]);

    // get specific search location
    String location = assetFolder;
    if (keyValue.containsKey(keysToGet[1])) {
      location = assetFolder + "/" + Utility.normalizePath(keyValue.get(keysToGet[1]));
      location = location.replaceAll("\\\\", "/");
    }

    // location = location.replaceAll("/app_assets", "");
    return new NounMetadata(
        GitAssetUtils.listAssetMetadata(location, search, assetFolder, null, null),
        PixelDataType.CUSTOM_DATA_STRUCTURE,
        PixelOperationType.OPERATION);
  }
}
