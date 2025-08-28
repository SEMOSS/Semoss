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

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClipboardReactor extends AbstractReactor {

  public ClipboardReactor() {
    this.keysToGet = new String[] {};
  }

  @Override
  public NounMetadata execute() {
    User user = this.insight.getUser();
    String showSource = null;
    if (user.getCtrlC() != null) showSource = user.getCtrlC().showSource;

    if (showSource != null) return NounMetadata.getSuccessNounMessage("Copied " + showSource);
    else return NounMetadata.getSuccessNounMessage("Clipboard is empty ");
  }
}
