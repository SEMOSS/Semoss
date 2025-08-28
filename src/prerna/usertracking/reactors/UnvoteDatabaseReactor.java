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
package prerna.usertracking.reactors;

import java.util.List;
import org.javatuples.Pair;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserCatalogVoteUtils;
import prerna.util.Utility;

@Deprecated
public class UnvoteDatabaseReactor extends AbstractReactor {

  public UnvoteDatabaseReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();

    if (Utility.isUserTrackingDisabled()) {
      return new NounMetadata(
          false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
    }

    List<Pair<String, String>> creds = User.getUserIdAndType(this.insight.getUser());

    String databaseId = this.keyValue.get(this.keysToGet[0]);
    if (databaseId == null) {
      throw new IllegalArgumentException("Database Id cannot be null.");
    }

    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
      throw new IllegalArgumentException("Database cannot be viewed by user.");
    }

    UserCatalogVoteUtils.delete(creds, databaseId);

    NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
    noun.addAdditionalReturn(
        NounMetadata.getSuccessNounMessage("Successfully unvoted for catalog"));
    return noun;
  }
}
