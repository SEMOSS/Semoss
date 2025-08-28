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
package prerna.reactor.task;

import java.util.List;
import java.util.Map;
import java.util.Set;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.insight.InsightUtility;

public class TaskOptionsReactor extends TaskBuilderReactor {

  private static final String IGNORE_PANEL_FILTERS = "ignoreFilters";

  public TaskOptionsReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.OPTIONS.getKey(), IGNORE_PANEL_FILTERS};
  }

  @Override
  protected void buildTask() {
    organizeKeys();
    List<Object> mapOptions = this.curRow.getValuesOfType(PixelDataType.MAP);
    if (mapOptions == null || mapOptions.size() == 0) {
      // if it is null, i guess we just clear the map values
      this.task.setTaskOptions(null);
    } else {
      this.task.setTaskOptions(new TaskOptions((Map<String, Object>) mapOptions.get(0)));

      // if we use task options on a panel
      // we automatically set the panel view to be visualization
      Set<String> panelIds = this.task.getTaskOptions().getPanelIds();
      for (String panelId : panelIds) {
        // we will store this as the last run for this panel
        // and start to merge in the panel filters that were applied
        if (task instanceof BasicIteratorTask) {
          SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
          this.insight.setFinalViewOptions(panelId, qs, task.getTaskOptions(), task.getFormatter());
          if (!ignorePanelFilters()) {
            qs.addPanel(this.insight.getInsightPanel(panelId));
          }
        }

        // and set panel for visualization
        InsightUtility.setPanelForVisualization(this.insight, panelId);
      }
    }
  }

  private boolean ignorePanelFilters() {
    GenRowStruct grs = this.store.getNoun(IGNORE_PANEL_FILTERS);
    if (grs != null && !grs.isEmpty()) {
      return Boolean.parseBoolean(grs.get(0) + "");
    }

    List<Object> booleanInputs = curRow.getValuesOfType(PixelDataType.BOOLEAN);
    if (booleanInputs != null && !booleanInputs.isEmpty()) {
      return (boolean) booleanInputs.get(0);
    }

    return false;
  }
}
