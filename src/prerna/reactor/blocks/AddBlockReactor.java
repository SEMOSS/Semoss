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
package prerna.reactor.blocks;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;

public class AddBlockReactor extends AbstractReactor {

  public AddBlockReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.NAME.getKey(),
          ReactorKeysEnum.SECTION.getKey(),
          ReactorKeysEnum.JSON.getKey()
        };
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    User user = this.insight.getUser();
    if (user == null) {
      NounMetadata noun =
          new NounMetadata(
              "User must be signed in to add a block",
              PixelDataType.CONST_STRING,
              PixelOperationType.ERROR,
              PixelOperationType.LOGGIN_REQUIRED_ERROR);
      SemossPixelException err = new SemossPixelException(noun);
      err.setContinueThreadOfExecution(false);
      throw err;
    }

    if (AbstractSecurityUtils.anonymousUsersEnabled()) {
      if (this.insight.getUser().isAnonymous()) {
        throwAnonymousUserError();
      }
    }

    boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
    if (!isAdmin) {
      throwFunctionalityOnlyExposedForAdminsError();
    }

    organizeKeys();
    Map<String, Object> blockDetails = getBlockDetails();
    String blockId = BlocksThemeUtils.addBlock(blockDetails);
    NounMetadata nm = new NounMetadata(blockId, PixelDataType.CONST_STRING);
    return nm;
  }

  // prepares map from inputs fields for use in block creation logic
  private Map<String, Object> getBlockDetails() {
    Map<String, Object> blockMap = new HashMap<>();
    blockMap.put("name", keyValue.get(ReactorKeysEnum.NAME.getKey()));
    blockMap.put("section", keyValue.get(ReactorKeysEnum.SECTION.getKey()));

    String rawJson = keyValue.get(ReactorKeysEnum.JSON.getKey());
    // Removes <encode> wrapper for json field
    if (rawJson != null) {
      try {
        rawJson = URLDecoder.decode(rawJson, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    blockMap.put("json", rawJson);
    blockMap.put("created_by", this.insight.getUserId());
    return blockMap;
  }
}
