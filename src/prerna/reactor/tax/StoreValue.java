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
package prerna.reactor.tax;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.InMemStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StoreValue extends AbstractReactor {

  private static final Logger LOGGER = LogManager.getLogger(StoreValue.class.getName());

  // TODO: find a common place to put these
  public static final String STORE_NOUN = "store";
  public static final String KEY_NOUN = "key";
  public static final String VALUE_NOUN = "value";

  /**
   * This reactor takes in 3 nouns store -> this points to the store name this will automatically be
   * replaced with the NounMetadata that is in the pixel planner
   *
   * <p>key -> this points to the unique key for the value we are adding to the store
   *
   * <p>value -> this is the value we are storing
   */
  @Override
  public NounMetadata execute() {
    NounMetadata storeNoun = (NounMetadata) this.store.getNoun(STORE_NOUN).getNoun(0);
    String key = this.store.getNoun(KEY_NOUN).get(0).toString();
    Object value = this.store.getNoun(VALUE_NOUN).get(0);
    PixelDataType valueType = this.store.getNoun(VALUE_NOUN).getMeta(0);
    // create a noun meta for the value to store
    NounMetadata valueData = new NounMetadata(value, valueType);

    InMemStore storeVariable = (InMemStore) storeNoun.getValue();
    storeVariable.put(key, valueData);
    LOGGER.info("Successfully stored " + key + " = " + value);

    return null;
  }

  @Override
  public List<NounMetadata> getInputs() {
    // if we are running this
    // any other situation which tries to change this value in the map
    // will end up recursively overriding with the existing value
    // if we add this to the plan
    // so we will just return null
    return null;
  }
}
