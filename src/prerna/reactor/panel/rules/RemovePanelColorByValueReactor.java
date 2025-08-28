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
package prerna.reactor.panel.rules;

import java.util.HashMap;
import java.util.Map;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemovePanelColorByValueReactor extends AbstractPanelColorByValueReactor {

  public RemovePanelColorByValueReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.NAME.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // get the insight panel
    InsightPanel insightPanel = getInsightPanel();
    if (insightPanel == null) {
      throw new NullPointerException("Could not find insight panel");
    }
    String cbvRule = getCbvId(1);
    if (cbvRule == null) {
      throw new NullPointerException("Must provide the color by value name within the panel");
    }
    boolean removed = insightPanel.removeColorByValue(cbvRule);
    if (!removed) {
      throw new NullPointerException("Could not find the color by value rule within the panel");
    }
    // need to return
    // panelId
    // cbvRuleId (name)
    // filter info of the qs

    Map<String, Object> retMap = new HashMap<String, Object>();
    retMap.put("panelId", insightPanel.getPanelId());
    retMap.put("name", cbvRule);
    return new NounMetadata(
        retMap,
        PixelDataType.CUSTOM_DATA_STRUCTURE,
        PixelOperationType.REMOVE_PANEL_COLOR_BY_VALUE);
  }

  public String getName() {
    return "RemovePanelColorByValue";
  }
}
