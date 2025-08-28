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

import java.util.List;
import java.util.Map;
import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractInsightPanelReactor extends AbstractReactor {

  protected static final String TRAVERSAL_KEY = ReactorKeysEnum.TRAVERSAL.getKey();

  protected InsightPanel getInsightPanel() {
    // passed in directly as panel
    GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
    if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
      NounMetadata noun = genericReactorGrs.getNoun(0);
      PixelDataType nounType = noun.getNounType();
      if (nounType == PixelDataType.PANEL) {
        return (InsightPanel) noun.getValue();
      } else if (nounType == PixelDataType.PANEL_CLONE_MAP) {
        Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
        return cloneMap.get("clone");
      } else if (nounType == PixelDataType.COLUMN
          || nounType == PixelDataType.CONST_STRING
          || nounType == PixelDataType.CONST_INT) {
        String panelId = noun.getValue().toString();
        return this.insight.getInsightPanel(panelId);
      }
    }

    // see if it is in the curRow
    // if it was passed directly in as a variable
    List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.PANEL);
    if (panelNouns != null && !panelNouns.isEmpty()) {
      return (InsightPanel) panelNouns.get(0).getValue();
    }

    // see if string or column passed in
    List<String> strInputs = this.curRow.getAllStrValues();
    if (strInputs != null && !strInputs.isEmpty()) {
      for (String panelId : strInputs) {
        InsightPanel panel = this.insight.getInsightPanel(panelId);
        if (panel != null) {
          return panel;
        }
      }
    }
    List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
    if (strNouns != null && !strNouns.isEmpty()) {
      return this.insight.getInsightPanel(strNouns.get(0).getValue().toString());
    }

    // see if a clone map was passed
    genericReactorGrs = this.store.getNoun(PixelDataType.PANEL_CLONE_MAP.toString());
    if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
      NounMetadata noun = genericReactorGrs.getNoun(0);
      Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
      return cloneMap.get("clone");
    }

    // see if it is in the curRow
    // if it was passed directly in as a variable
    panelNouns = this.curRow.getNounsOfType(PixelDataType.PANEL_CLONE_MAP);
    if (panelNouns != null && !panelNouns.isEmpty()) {
      NounMetadata noun = genericReactorGrs.getNoun(0);
      Map<String, InsightPanel> cloneMap = (Map<String, InsightPanel>) noun.getValue();
      return cloneMap.get("clone");
    }

    // well, you are out of luck
    return null;
  }

  protected String getTraversalLiteralInput() {
    // see if it was passed directly in with the lower case key ornaments
    GenRowStruct genericReactorGrs = this.store.getNoun(TRAVERSAL_KEY);
    if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
      return genericReactorGrs.get(0).toString();
    }

    // see if it is in the curRow
    // if it was passed directly in as a variable
    List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
    if (strNouns != null && !strNouns.isEmpty()) {
      return strNouns.get(0).getValue().toString();
    }

    // well, you are out of luck
    return null;
  }
}
