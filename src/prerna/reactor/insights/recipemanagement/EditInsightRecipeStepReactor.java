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

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import prerna.om.Pixel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class EditInsightRecipeStepReactor extends AbstractReactor {

  private static final String CLASS_NAME = EditInsightRecipeStepReactor.class.getName();

  public EditInsightRecipeStepReactor() {
    this.keysToGet =
        new String[] {ReactorKeysEnum.PIXEL_ID.getKey(), ReactorKeysEnum.PIXEL.getKey()};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    String pixelId = this.keyValue.get(this.keysToGet[0]);
    String encodedRecipe = this.keyValue.get(this.keysToGet[1]);
    if (pixelId == null || pixelId.isEmpty()) {
      throw new NullPointerException("Pixel id cannot be null");
    }
    if (encodedRecipe == null || encodedRecipe.isEmpty()) {
      throw new NullPointerException("Pixel string for the substitution cannot be null");
    }

    Logger logger = getLogger(CLASS_NAME);
    String recipe = Utility.decodeURIComponent(encodedRecipe);
    Pixel p = this.insight.getPixelList().getPixel(pixelId);
    p.modifyPixelString(recipe);

    // now i need to rerun the insight recipe
    // clear the insight
    // and re-run it
    logger.info(
        "Re-executing the insight recipe... please wait as this operation may take some time");
    PixelRunner runner = this.insight.reRunPixelInsight(false);
    // return the recipe steps
    Map<String, Object> runnerWraper = new HashMap<String, Object>();
    runnerWraper.put("runner", runner);
    NounMetadata noun =
        new NounMetadata(
            runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.RERUN_INSIGHT_RECIPE);
    return noun;
  }
}
