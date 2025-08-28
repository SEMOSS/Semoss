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
package prerna.sablecc2.om.task.options;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import prerna.sablecc2.om.NounStore;

public class TaskOptions {

  /*
   * Please note - this is basically a wrapper around the Map<String, Object> options
   * The keys to this map should always be panel ids
   * Do not add random keys inside - this will break other logic that assumes that we can determine
   * the panel that this task is running on
   */

  private Map<String, Object> options;
  private boolean ornament = false;
  // kinda hacky at the moment
  private NounStore collectStore = null;

  /**
   * Constructor for task options
   *
   * @param options
   */
  public TaskOptions(Map<String, Object> options) {
    this.options = options;
  }

  public Set<String> getPanelIds() {
    return this.options.keySet();
  }

  public boolean isOrnament() {
    return this.ornament;
  }

  public void setOrnament(boolean ornament) {
    this.ornament = ornament;
  }

  public Map<String, Object> getAlignmentMap(String panelId) {
    Object pOptions = this.options.get(panelId);
    if (pOptions != null && pOptions instanceof Map) {
      Map<String, Object> panelOptions = (Map<String, Object>) pOptions;
      if (panelOptions != null && panelOptions.containsKey("alignment")) {
        return (Map<String, Object>) panelOptions.get("alignment");
      }
    }
    return null;
  }

  public String getLayout(String panelId) {
    // this is what I need to change for layout
    Object pOptions = this.options.get(panelId);
    if (pOptions != null && pOptions instanceof Map) {
      Map<String, Object> panelOptions = (Map<String, Object>) pOptions;
      if (panelOptions != null && panelOptions.containsKey("layout")) {
        return (String) panelOptions.get("layout");
      }
    }
    return null;
  }

  public String getPanelLayerId(String panelId) {
    Object pOptions = this.options.get(panelId);
    if (pOptions != null && pOptions instanceof Map) {
      Map<String, Object> panelOptions = (Map<String, Object>) pOptions;
      if (panelOptions != null && panelOptions.containsKey("layer")) {
        Map<String, String> layerOptions = (Map<String, String>) panelOptions.get("layer");
        if (layerOptions != null) {
          return layerOptions.get("id");
        }
      }
    }
    return null;
  }

  public Map<String, Object> getOptions() {
    return this.options;
  }

  public boolean isEmpty() {
    return this.options.isEmpty();
  }

  /**
   * Swap the current panel ids Boolean to pass if to remove all the other panels and keep only the
   * new panelId
   *
   * @param newPanelId
   * @param existingPanelId
   */
  public void swapPanelIds(String newPanelId, String curPanelId) {
    Map<String, Object> newOptions = new HashMap<>();
    newOptions.put(newPanelId, this.options.get(curPanelId));
    this.options = newOptions;
  }

  /**
   * Set the noun store that was used during the collect
   *
   * @param collectStore
   */
  public void setCollectStore(NounStore collectStore) {
    this.collectStore = collectStore;
  }

  /**
   * Get the collect store
   *
   * @return
   */
  public NounStore getCollectStore() {
    return collectStore;
  }
}
