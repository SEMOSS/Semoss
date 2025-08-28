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
package prerna.reactor.insights.recipemanagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCurrentRecipeReactor extends AbstractReactor {

  @Override
  public NounMetadata execute() {
    PixelList pixelList = this.insight.getPixelList();

    List<Map<String, Object>> retList = new ArrayList<>();
    // Don't add GetCurrentRecipe to the end
    for (int i = 0; i < pixelList.size(); i++) {
      Pixel p = pixelList.get(i);
      Map<String, Object> innerMap = new HashMap<>();
      innerMap.put("id", p.getId());
      innerMap.put("expression", p.getPixelString());
      innerMap.put("error", p.isReturnedError());
      innerMap.put("errorMessages", p.getErrorMessages());
      innerMap.put("warning", p.isReturnedWarning());
      innerMap.put("warningMessages", p.getWarningMessages());
      retList.add(innerMap);
    }
    return new NounMetadata(retList, PixelDataType.MAP, PixelOperationType.CURRENT_INSIGHT_RECIPE);
  }
}
