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

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PanelExistsReactor extends AbstractReactor {

  public PanelExistsReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // first input is the name of the panel
    String panelId = this.curRow.get(0).toString();
    // determine if the id currently exists
    boolean panelExists = this.insight.getInsightPanels().containsKey(panelId);
    return new NounMetadata(panelExists, PixelDataType.BOOLEAN);
  }
}
