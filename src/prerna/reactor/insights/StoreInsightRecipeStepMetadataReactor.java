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
package prerna.reactor.insights;

import java.util.HashMap;
import java.util.Map;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StoreInsightRecipeStepMetadataReactor extends AbstractReactor {

  public StoreInsightRecipeStepMetadataReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PIXEL_ID.getKey(),
          ReactorKeysEnum.ALIAS.getKey(),
          ReactorKeysEnum.DESCRIPTION.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String pixelId = this.keyValue.get(this.keysToGet[0]);
    String alias = this.keyValue.get(this.keysToGet[1]);
    String description = this.keyValue.get(this.keysToGet[2]);

    PixelList pixelList = this.insight.getPixelList();
    Pixel pixelObj = null;
    if (pixelId == null || pixelId.isEmpty()) {
      int size = pixelList.size();
      pixelObj = this.insight.getPixelList().get(size - 1);
      pixelId = pixelObj.getId();
    }

    if (pixelObj != null) {
      pixelObj.setPixelAlias(alias);
      pixelObj.setPixelDescription(description);
    } else {
      pixelObj = pixelList.getPixel(pixelId);
      if (pixelObj != null) {
        pixelObj.setPixelAlias(alias);
        pixelObj.setPixelDescription(description);
      } else {
        return getWarning("Unable to find pixelId = " + pixelId);
      }
    }

    Map<String, String> newValues = new HashMap<>();
    newValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), pixelId);
    newValues.put(ReactorKeysEnum.ALIAS.getKey(), alias);
    newValues.put(ReactorKeysEnum.DESCRIPTION.getKey(), description);
    return new NounMetadata(newValues, PixelDataType.MAP);
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(ReactorKeysEnum.ALIAS.getKey())) {
      return "The alias to assign for the pixel recipe step";
    } else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
      return "The description to provide for the pixel recipe step";
    }
    return super.getDescriptionForKey(key);
  }
}
