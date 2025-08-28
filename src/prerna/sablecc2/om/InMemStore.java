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
package prerna.sablecc2.om;

import java.util.Set;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;

public interface InMemStore<K, V> {

  /**
   * Default iterator
   *
   * @return
   */
  public IRawSelectWrapper getIterator();

  /**
   * Iterator with qs defined
   *
   * @param qs
   * @return
   */
  public IRawSelectWrapper getIterator(SelectQueryStruct qs);

  /**
   * Insert data to be stored
   *
   * @param key
   * @param val
   */
  public void put(K key, V value);

  /**
   * Get data that is stored
   *
   * @param key
   * @return
   */
  public V get(K key);

  /**
   * @param gets the evaluated value of the data stored in the key
   * @return
   */
  public V getEvaluatedValue(K key);

  /**
   * Remove data that is stored
   *
   * @param key
   * @return
   */
  public V remove(K key);

  /**
   * Returns whether the key exists in the mem store
   *
   * @param key
   * @return
   */
  public boolean containsKey(K key);

  /**
   * Get the set of keys currently stored
   *
   * @return
   */
  public Set<K> getKeys();

  /** clears the object of all keys and values */
  public void clear();
}
