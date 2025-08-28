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
package prerna.reactor.panel.sort;

import java.util.List;
import prerna.om.InsightPanel;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPanelSortReactor extends AbstractPanelSortReactor {

  public GetPanelSortReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey()};
  }

  @Override
  public NounMetadata execute() {
    InsightPanel panel = getInsightPanel();
    if (panel == null) {
      throw new NullPointerException("Panel must be defined to retrieve the sorts");
    }
    // pull and send the panel sorts
    List<IQuerySort> orderBys = panel.getPanelOrderBys();
    NounMetadata noun = new NounMetadata(orderBys, PixelDataType.CUSTOM_DATA_STRUCTURE);
    return noun;
  }
}
