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
package prerna.reactor.browser;

import java.util.HashMap;
import java.util.Map;
import org.json.JSONObject;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FillInputReactor extends AbstractReactor {

  private static final String REACTOR_DESCRIPTION =
      "Fill the currently selected input of the Browser App rendered on the server.";
  private static final String INPUT_KEY_DESCRIPTION =
      "The text to fill the currently selected input of the browser app rendered on the server.";

  public FillInputReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.INPUT.getKey()};
    this.keyRequired = new int[] {1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();

    BrowserUtils.ensureUserLoggedIn(user);

    if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
      throwAnonymousUserError();
    }

    String input = this.keyValue.get(this.keysToGet[0]);

    // Ideally, previous call would have been ClickXY and the form to be input would already
    // Be selected. That way, the next action would be. FillInputForm.

    Map<String, Object> actions = new HashMap<>();

    actions.put("actor", "system");
    actions.put("action", "fill");
    actions.put("event", "fill");
    actions.put("fill_value", input);

    String json = BrowserUtils.mapToJsonString(actions);

    JSONObject jo = new JSONObject(json);
    PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
    if (pbu == null) {
      throw new IllegalArgumentException(
          "There is no Playwright Browser currently open for this insight.");
    }
    pbu.enterInput(input);
    // pbu.fill(jo);

    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }

  @Override
  public String getReactorDescription() {
    return REACTOR_DESCRIPTION;
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.INPUT.getKey())) {
      return INPUT_KEY_DESCRIPTION;
    } else {
      return super.getDescriptionForKey(key);
    }
  }
}
