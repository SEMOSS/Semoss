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
package prerna.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddPanelConfigReactor extends AbstractInsightPanelReactor {

  // input keys for the map
  @Deprecated private static final String CONFIG = "config";

  public AddPanelConfigReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.CONFIG.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // get the insight panel
    InsightPanel insightPanel = getInsightPanel();
    if (insightPanel == null) {
      throw new IllegalArgumentException("Cannot find the insight panel");
    }
    // get the map
    Map<String, Object> mapInput = getMapInput();
    if (mapInput == null) {
      throw new IllegalArgumentException("Need to define the config input");
    }
    Map<String, Object> config = null;
    // deprecated logic
    if (mapInput.containsKey(CONFIG)) {
      config = (Map<String, Object>) mapInput.get(CONFIG);
      if (config == null) {
        config = new HashMap<String, Object>();
      }
    }
    // the input should always be exactly what we want to input
    else {
      config = mapInput;
    }
    // merge the map options
    insightPanel.addConfig(config);
    return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_CONFIG);
  }

  private Map<String, Object> getMapInput() {
    // see if it was passed directly in with the lower case key config
    GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
    if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
      return (Map<String, Object>) genericReactorGrs.get(0);
    }

    // see if it is in the curRow
    // if it was passed directly in as a variable
    List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (panelNouns != null && !panelNouns.isEmpty()) {
      return (Map<String, Object>) panelNouns.get(0).getValue();
    }

    // well, you are out of luck
    return null;
  }
}
