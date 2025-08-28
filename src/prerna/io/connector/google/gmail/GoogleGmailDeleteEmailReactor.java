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
package prerna.io.connector.google.gmail;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleGmailDeleteEmailReactor extends AbstractReactor {

  private static final Logger classLogger =
      LogManager.getLogger(GoogleGmailDeleteEmailReactor.class);

  private static final String STATUS_KEY = "status";

  public GoogleGmailDeleteEmailReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.ID.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    this.organizeKeys();
    String id = this.keyValue.get(this.keysToGet[0]);
    try {
      User user = this.insight.getUser();
      String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
      boolean result = GoogleGmailHelper.deleteEmail(accessToken, id);
      Map<String, Object> map = new HashMap<>();
      map.put(STATUS_KEY, result);
      return new NounMetadata(
          map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
    } catch (SemossPixelException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw e;
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new SemossPixelException(
          "An error occurred deleting the email. Error message: " + e.getMessage());
    }
  }

  @Override
  public String getReactorDescription() {
    return "Delete an email";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.ID.getKey())) {
      return "Unique identifier of the Google email to be deleted " + ReactorKeysEnum.ID.getKey();
    }
    return super.getDescriptionForKey(key);
  }
}
