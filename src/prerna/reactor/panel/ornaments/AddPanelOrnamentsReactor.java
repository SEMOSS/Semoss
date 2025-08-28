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
package prerna.reactor.panel.ornaments;

import java.util.List;
import java.util.Map;
import prerna.om.InsightPanel;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddPanelOrnamentsReactor extends AbstractInsightPanelReactor {

  public AddPanelOrnamentsReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.ORNAMENTS_KEY.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // get the insight panel
    InsightPanel insightPanel = getInsightPanel();
    // get the ornaments that come as a map
    Map<String, Object> ornaments = getOrnamentsMapInput();
    if (ornaments == null) {
      throw new IllegalArgumentException("Need to define the ornament input");
    }
    // merge the map options
    insightPanel.addOrnaments(ornaments);
    return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_ORNAMENT);
  }

  private Map<String, Object> getOrnamentsMapInput() {
    // see if it was passed directly in with the lower case key ornaments
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
