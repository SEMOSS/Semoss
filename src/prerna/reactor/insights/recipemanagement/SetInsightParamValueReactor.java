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

import java.util.List;
import java.util.Vector;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetInsightParamValueReactor extends AbstractInsightParameterReactor {

  private static final String PARAM_NAME = "paramName";
  private static final String PARAM_VALUE = "paramValue";

  public SetInsightParamValueReactor() {
    this.keysToGet = new String[] {PARAM_NAME, PARAM_VALUE};
  }

  @Override
  public NounMetadata execute() {
    String paramName = getParamName();
    List<Object> paramValues = getParamValue();
    if (paramValues.isEmpty()) {
      return new NounMetadata(false, PixelDataType.BOOLEAN);
    }
    Object setValue = paramValues;
    if (paramValues.size() == 1) {
      setValue = paramValues.get(0);
    }

    // fill this in for the param struct
    String variableName = VarStore.PARAM_STRUCT_PREFIX + paramName;
    NounMetadata paramNoun = this.insight.getVarStore().get(variableName);
    if (paramNoun == null || (paramNoun.getNounType() != PixelDataType.PARAM_STRUCT)) {
      // will make my own param struct
      // and also fill in
      ParamStruct p = new ParamStruct();
      p.setParamName(paramName);
      ParamStructDetails d = new ParamStructDetails();
      d.setCurrentValue(setValue);
      p.addParamStructDetails(d);
      // store in the insight
      paramNoun = new NounMetadata(p, PixelDataType.PARAM_STRUCT);
      this.insight.getVarStore().put(variableName, paramNoun);
      // still just return false to denote it wasn't pre-existing
      return new NounMetadata(false, PixelDataType.BOOLEAN);
    }

    ParamStruct pStruct = (ParamStruct) paramNoun.getValue();
    List<ParamStructDetails> details = pStruct.getDetailsList();
    for (ParamStructDetails detail : details) {
      detail.setCurrentValue(setValue);
    }

    return new NounMetadata(true, PixelDataType.BOOLEAN);
  }

  /**
   * Get the param name
   *
   * @return
   */
  private String getParamName() {
    GenRowStruct grs = this.store.getNoun(PARAM_NAME);
    if (grs != null && !grs.isEmpty()) {
      return grs.get(0).toString();
    }

    if (!this.curRow.isEmpty()) {
      return this.curRow.get(0).toString();
    }

    throw new IllegalArgumentException("Must pass in the parameter name");
  }

  /**
   * Get the param values passed in
   *
   * @return
   */
  private List<Object> getParamValue() {
    List<Object> values = new Vector<Object>();
    GenRowStruct paramValue = this.store.getNoun(PARAM_VALUE);
    if (paramValue != null && !paramValue.isEmpty()) {
      for (int i = 0; i < paramValue.size(); i++) {
        values.add(paramValue.getNoun(i).getValue());
      }
    }

    return values;
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(PARAM_NAME)) {
      return "The name of the param";
    } else if (key.equals(PARAM_VALUE)) {
      return "The value of the param";
    }
    return super.getDescriptionForKey(key);
  }
}
