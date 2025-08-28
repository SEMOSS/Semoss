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
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FilterReactor extends AbstractReactor {

  public NounMetadata execute() {
    // the translation will set each component
    // under a different noun
    // we have 3 nouns:
    // LCOL, RCOL, COMPARATOR
    // return a new NounMetadata with the filter

    GenRowStruct lcol = store.getNoun("LCOL");
    GenRowStruct comparator = store.getNoun("COMPARATOR");
    GenRowStruct rcol = store.getNoun("RCOL");

    Filter thisFilter = new Filter(lcol, comparator.get(0).toString(), rcol);
    thisFilter.setVarStore(this.insight.getVarStore());
    NounMetadata filterNoun = new NounMetadata(thisFilter, PixelDataType.FILTER);
    return filterNoun;
  }

  @Override
  public void mergeUp() {
    // we need to push the filter object to the parent
    // creation of the filter object is lazy
    // so it doesn't evaluate yet
    // so we will just call execute to get the object
    NounMetadata filterNoun = (NounMetadata) execute();
    // and we will just push this into the cur row of the parent
    this.parentReactor.getCurRow().add(filterNoun);
  }

  @Override
  public List<NounMetadata> getInputs() {
    // we do not want this to be added to the planner
    // as its own OP
    // since a filter is only useful when evaluated
    // within another expression
    // we will have merge up handle this
    return null;
  }

  //	@Override
  //	public List<NounMetadata> getOutputs() {
  //		// we do not want this to be added to the planner
  //		// as its own OP
  //		// since a filter is only useful when evaluated
  //		// within another expression
  //		// we will have merge up handle this
  //		return null;
  //	}
}
