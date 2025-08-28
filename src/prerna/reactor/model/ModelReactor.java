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
package prerna.reactor.model;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.modelinference.ModelInferenceQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ModelReactor extends AbstractQueryStructReactor {

  public ModelReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.MODEL.getKey()};
  }

  @Override
  protected AbstractQueryStruct createQueryStruct() {
    this.organizeKeys();

    String modelId = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());

    // we may have the alias
    modelId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), modelId);
    if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), modelId)) {
      throw new IllegalArgumentException(
          "Model " + modelId + " does not exist or user does not have access to it");
    }

    this.qs.setEngineId(modelId);
    this.qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);

    return this.qs;
  }

  // initialize the reactor with its necessary inputs
  @Override
  protected void init() {
    // this will happen when we have an explicit querystruct
    // or one result piped a query struct to the current reactor
    GenRowStruct qsInputParams = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.getKey());
    if (qsInputParams != null) {
      int numInputs = qsInputParams.size();
      for (int inputIdx = 0; inputIdx < numInputs; inputIdx++) {
        NounMetadata qsNoun = (NounMetadata) qsInputParams.getNoun(inputIdx);
        AbstractQueryStruct qs = (AbstractQueryStruct) qsNoun.getValue();
        mergeQueryStruct(qs);
      }
    }

    // if it is not piped
    // but there is a query struct within a query struct
    // the specific instance of the reactor will handle those types of merges
    // example
    // selector ( studio , sum(mb) )
    // the selector reactor will handle putting the studio and the sum(mb)

    if (this.qs == null) {
      this.qs = new ModelInferenceQueryStruct();
    }
  }
}
