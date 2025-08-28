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
package prerna.reactor.prompt;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.prompt.PromptUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckPromptTitleReactor extends AbstractReactor {

  public CheckPromptTitleReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROMPT_TITLE.getKey()};
  }

  @Override
  public NounMetadata execute() {
    User user = this.insight.getUser();
    if (user == null) {
      NounMetadata noun =
          new NounMetadata(
              "User must be signed into an account in order to create prompt",
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

    organizeKeys();
    String promptTitle = this.keyValue.get(this.keysToGet[0]);
    Boolean promptTitleUsed = PromptUtils.checkPromptTitle(promptTitle);
    NounMetadata nm = new NounMetadata(promptTitleUsed, PixelDataType.BOOLEAN);
    return nm;
  }
}
