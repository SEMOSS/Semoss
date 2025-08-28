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

import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddPanelIfAbsentReactor extends AbstractReactor {

  public AddPanelIfAbsentReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.SHEET.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // first input is the name of the panel
    organizeKeys();
    String panelId = this.keyValue.get(this.keysToGet[0]);
    InsightPanel panel = this.insight.getInsightPanels().get(panelId);
    if (panel == null) {
      String sheetId = this.keyValue.get(this.keysToGet[1]);
      if (sheetId == null) {
        sheetId = Insight.DEFAULT_SHEET_ID;
      }
      panel = new InsightPanel(panelId, sheetId);
      this.insight.addNewInsightPanel(panel);
      NounMetadata noun =
          new NounMetadata(panel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
      return noun;
    }
    NounMetadata noun =
        new NounMetadata(panel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN_IF_ABSENT);
    return noun;
  }
}
