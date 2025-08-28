/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.browser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClickXYReactor extends AbstractReactor {

  private static final String REACTOR_DESCRIPTION =
      "Click on the x, y coordinate of the Browser App rendered on the server.";
  private static final String X_KEY_DESCRIPTION =
      "The X coordiante of the Browser App rendered on the server.";
  private static final String Y_KEY_DESCRIPTION =
      "The Y coordinate of the Browser App rendered on the server.";

  public ClickXYReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.X.getKey(), ReactorKeysEnum.Y.getKey()};
    this.keyRequired = new int[] {1, 1};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();

    BrowserUtils.ensureUserLoggedIn(user);

    if (BrowserUtils.anonymousEnabledAndUserAnonymous(user)) {
      throwAnonymousUserError();
    }

    int x = Integer.parseInt(this.keyValue.get(this.keysToGet[0]));
    int y = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));

    // We would map the x,y coordinate that we clicked in the UI to the browser being
    // rendered locally.

    Map<String, Object> actions = new HashMap<>();
    actions.put("actor", "system");
    actions.put("action", "clickXY");
    actions.put("event", "click");

    List<Integer> params = new ArrayList<>();
    params.add(x);
    params.add(y);
    actions.put("params", params);

    String json = BrowserUtils.mapToJsonString(actions);

    JSONObject jo = new JSONObject(json);

    PlaywrightBrowserUtil pbu = this.insight.getPlaywrightUtil();
    if (pbu == null) {
      throw new IllegalArgumentException(
          "There is no Playwright Browser currently open for this insight.");
    }
    pbu.mouse_xy(jo, "clickXY");

    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }

  @Override
  public String getReactorDescription() {
    return REACTOR_DESCRIPTION;
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.X.getKey())) {
      return X_KEY_DESCRIPTION;
    } else if (key.equals(ReactorKeysEnum.Y.getKey())) {
      return Y_KEY_DESCRIPTION;
    } else {
      return super.getDescriptionForKey(key);
    }
  }
}
