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

import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.logging.log4j.Logger;
import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class RefreshAllPanelTasksReactor extends AbstractReactor {

  private static final String CLASS_NAME = RefreshAllPanelTasksReactor.class.getName();

  public RefreshAllPanelTasksReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.LIMIT.getKey()};
  }

  @Override
  public NounMetadata execute() {
    Logger logger = getLogger(CLASS_NAME);
    // store the tasks to reset
    List<NounMetadata> taskOutput = new Vector<NounMetadata>();
    // get the limit for the new tasks
    Integer limit = getTotalToCollect();

    List<NounMetadata> additionalMessages = new Vector<>();

    Map<String, InsightPanel> insightPanelsMap = this.insight.getInsightPanels();
    for (String panelId : insightPanelsMap.keySet()) {
      InsightPanel panel = insightPanelsMap.get(panelId);
      if (!panel.getPanelView().equalsIgnoreCase("visualization")) {
        continue;
      }
      Integer panelCollect = limit;
      if (panelCollect == null) {
        panelCollect = panel.getNumCollect();
      }
      // need to account for layers
      // so will loop through the layer maps
      // that we are storing
      InsightUtility.refreshPanelTasks(
          this.insight, panel, panelCollect, taskOutput, additionalMessages, logger);
    }

    NounMetadata noun =
        new NounMetadata(taskOutput, PixelDataType.TASK_LIST, PixelOperationType.RESET_PANEL_TASKS);
    if (!additionalMessages.isEmpty()) {
      noun.addAllAdditionalReturn(additionalMessages);
    }
    return noun;
  }

  // returns how much do we need to collect
  private Integer getTotalToCollect() {
    // try the key
    GenRowStruct numGrs = store.getNoun(keysToGet[0]);
    if (numGrs != null && !numGrs.isEmpty()) {
      return ((Number) numGrs.get(0)).intValue();
    }

    return null;
  }
}
