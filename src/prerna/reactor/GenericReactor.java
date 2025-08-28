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

import java.util.List;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenericReactor extends AbstractReactor {

  public GenericReactor() {
    setName("Generic");
  }

  @Override
  public NounMetadata execute() {
    // THIS IS A SPECIAL CASE
    // we want to merge up into the parent
    // but unlike the mergeup routine
    // we want to replace anything that is a variable
    // with the actual object
    String key = (String) store.getNoun("KEY").get(0).toString();

    GenRowStruct allNouns = store.getNoun(NounStore.all);
    GenRowStruct thisStruct;
    if (store.getNoun(key) == null) {
      thisStruct = store.makeNoun(key);
    } else {
      thisStruct = store.getNoun(key);
    }

    int numNouns = allNouns.size();
    for (int nounIdx = 0; nounIdx < numNouns; nounIdx++) {
      NounMetadata thisNoun = allNouns.getNoun(nounIdx);
      Object nounValue = thisNoun.getValue();
      PixelDataType nounType = thisNoun.getNounType();
      List<PixelOperationType> nounOps = thisNoun.getOpType();
      if (nounType == PixelDataType.COLUMN) {
        NounMetadata value = this.planner.getVariableValue((String) nounValue);
        if (value != null) {
          thisStruct.add(value);
        } else {
          thisStruct.add(nounValue, nounType);
        }
      } else {
        thisStruct.add(thisNoun);
      }
    }

    // just add this to the parent
    parentReactor.getNounStore().addNoun(key, thisStruct);
    return null;
  }

  @Override
  public void mergeUp() {
    String key = (String) store.getNoun("KEY").get(0).toString();

    GenRowStruct allNouns = store.getNoun(NounStore.all);
    GenRowStruct thisStruct;
    if (store.getNoun(key) == null) {
      thisStruct = store.makeNoun(key);
    } else {
      thisStruct = store.getNoun(key);
    }

    int numNouns = allNouns.size();
    for (int nounIdx = 0; nounIdx < numNouns; nounIdx++) {
      thisStruct.add(allNouns.getNoun(nounIdx));
    }

    // just add this to the parent
    parentReactor.getNounStore().addNoun(key, thisStruct);
  }

  @Override
  public List<NounMetadata> getInputs() {
    // this is used primarily for the planner
    // we do not need to add these steps since
    // the parent will automatically take these
    // into consideration
    return null;
  }

  @Override
  public List<NounMetadata> getOutputs() {
    // this is used primarily for the planner
    // we do not need to add these steps since
    // the parent will automatically take these
    // into consideration
    return null;
  }
}
