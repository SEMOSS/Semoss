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

import java.sql.SQLException;
import java.util.List;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.theme.BlocksThemeUtils;
import prerna.theme.ThemeDbTable;

public class DeleteBlockReactor extends AbstractReactor {

  public DeleteBlockReactor() {
    this.keysToGet = new String[] {"blockId", "hardDelete"};
    this.keyRequired = new int[] {1, 0};
  }

  @Override
  public NounMetadata execute() {

    User user = this.insight.getUser();
    if (user == null) {
      NounMetadata noun =
          new NounMetadata(
              "User must be signed in to delete a block",
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

    this.organizeKeys();
    boolean hardDelete = false;
    GenRowStruct grs = this.store.getNoun("hardDelete");
    if (grs != null && !grs.isEmpty()) {
      List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.BOOLEAN);
      if (mapNouns != null && !mapNouns.isEmpty()) {
        hardDelete = (boolean) mapNouns.get(0).getValue();
      }
    }
    String blockId = this.keyValue.get("blockId");
    String tableName = ThemeDbTable.BLOCKS_TABLE.toString();
    boolean result = false;
    try {
      result = BlocksThemeUtils.deleteBlock(blockId, tableName, hardDelete);
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return new NounMetadata(result, PixelDataType.BOOLEAN);
  }
}
