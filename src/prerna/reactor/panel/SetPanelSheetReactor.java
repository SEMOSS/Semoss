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
import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetPanelSheetReactor extends AbstractInsightPanelReactor {

  public SetPanelSheetReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.SHEET.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // get the insight panel
    InsightPanel insightPanel = getInsightPanel();
    // get the ornaments that come as a map
    String panelSheet = getPanelSheet();
    if (panelSheet == null) {
      throw new IllegalArgumentException("Must pass in a sheet id for the insight");
    }
    if (this.insight.getInsightSheet(panelSheet) == null) {
      throw new IllegalArgumentException("Sheet id = " + panelSheet + " does not exist");
    }
    // merge the map options
    insightPanel.setSheetId(panelSheet);
    return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_LABEL);
  }

  private String getPanelSheet() {
    // see if it was passed directly in with the lower case key ornaments
    GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
    if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
      return genericReactorGrs.get(0).toString();
    }

    // see if it is in the curRow
    // if it was passed directly in as a variable
    List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
    if (strNouns != null && !strNouns.isEmpty()) {
      return strNouns.get(0).getValue().toString();
    }

    return null;
  }
}
