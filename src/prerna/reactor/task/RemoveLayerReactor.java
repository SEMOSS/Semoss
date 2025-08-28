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
package prerna.reactor.task;

import java.util.HashMap;
import java.util.Map;
import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveLayerReactor extends AbstractReactor {

  public RemoveLayerReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.LAYER.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    // get the task id
    String panelId = this.keyValue.get(this.keysToGet[0]);
    String layerId = this.keyValue.get(this.keysToGet[1]);

    Map<String, String> removeLayerMap = new HashMap<String, String>();
    removeLayerMap.put("panel", panelId);
    removeLayerMap.put("layer", layerId);

    // remove from the insight panel store
    InsightPanel panel = this.insight.getInsightPanel(panelId);
    if (panel == null) {
      throw new NullPointerException("Panel " + panelId + " does not exist");
    }
    panel.removeLayerViewOptions(layerId);

    return new NounMetadata(
        removeLayerMap, PixelDataType.REMOVE_LAYER, PixelOperationType.REMOVE_LAYER);
  }
}
