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
package prerna.reactor.panel;

import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MovePanelReactor extends AbstractInsightPanelReactor {

  public MovePanelReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.SHEET.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    // get the insight panel
    InsightPanel existingPanel = getInsightPanel();
    String sheetId = this.keyValue.get(this.keysToGet[1]);
    if (sheetId == null) {
      throw new NullPointerException("Must define the sheet where the panel should be moved");
    }
    if (existingPanel.getSheetId().equals(sheetId)) {
      throw new IllegalArgumentException(
          "The sheet passed is the same as the panels current sheet");
    }
    existingPanel.setSheetId(sheetId);
    return new NounMetadata(existingPanel, PixelDataType.PANEL, PixelOperationType.PANEL_MOVE);
  }
}
