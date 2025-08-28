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
package prerna.reactor;

import java.util.HashMap;
import java.util.Map;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * This is just a internal class so that when we compile to execute the assimilator we can have a
 * method to call based on the super that is assigned to the new class
 */
public abstract class AssimilatorEvaluator extends AbstractReactor {

  public Map<String, Object> vars = new HashMap<>();
  public boolean containsStringValue = false;
  public boolean allIntValue = true;

  public AssimilatorEvaluator() {}

  @Override
  public NounMetadata execute() {
    NounMetadata noun = null;
    Object retVal = getExpressionValue();
    if (containsStringValue) {
      noun = new NounMetadata(retVal.toString(), PixelDataType.CONST_STRING);
    } else if (allIntValue) {
      Number result = (Number) retVal;
      if (result.doubleValue() == Math.rint(result.doubleValue())) {
        noun = new NounMetadata(((Number) retVal).intValue(), PixelDataType.CONST_INT);
      } else {
        // not a valid integer
        // return as a double
        noun = new NounMetadata(((Number) retVal).doubleValue(), PixelDataType.CONST_DECIMAL);
      }
    } else {
      noun = new NounMetadata(((Number) retVal).doubleValue(), PixelDataType.CONST_DECIMAL);
    }

    return noun;
  }

  /**
   * Method that return the evaluation of the signature
   *
   * @return
   */
  public abstract Object getExpressionValue();

  public void setVars(Map<String, Object> vars) {
    this.vars = vars;
  }

  public void setVar(String key, Object value) {
    this.vars.put(key, value);
  }

  public Object getVar(String key) {
    return this.vars.get(key);
  }
}
