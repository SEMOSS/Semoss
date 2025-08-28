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
package prerna.reactor.export;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import prerna.engine.api.IHeadersDataRow;
import prerna.util.ArrayUtilityMethods;

public class HierarchyFormatter extends AbstractFormatter {

  public static final String PATH_KEY = "path";

  private static final String CHILDREN = "children";
  private static final String PARENT = "parent";
  private static final String NAME = "name";
  private static final String NULL_PLACEHOLDER = "EMPTY_VALUE";

  private List<Object[]> data;
  private String[] headers;

  public HierarchyFormatter() {
    this.data = new ArrayList<Object[]>(100);
  }

  public HierarchyFormatter(List<Object[]> data, String[] headers) {
    this.data = data;
    this.headers = headers;
  }

  @Override
  public void addData(IHeadersDataRow nextData) {
    this.headers = nextData.getHeaders();
    this.data.add(nextData.getValues());
  }

  @Override
  public void clear() {
    this.data = new ArrayList<Object[]>(100);
    this.headers = null;
  }

  @Override
  public Object getFormattedData() {
    List<String> path = (List<String>) this.optionsMap.get(PATH_KEY);
    return getHierarchyData(this.data, this.headers, path);
  }

  private Map<Object, Object> getHierarchyData(
      List<Object[]> data, String[] headers, List<String> path) {
    // need to have 1 dendrogram map
    LinkedHashMap<Object, Object> hierarchyMap = new LinkedHashMap<Object, Object>();
    hierarchyMap.put(NAME, "root");
    hierarchyMap.put(PARENT, "null");
    hierarchyMap.put(CHILDREN, new LinkedHashSet<Map<Object, Object>>());

    // loop through to get the indices of the datarow we care about for the various components
    int num_components = path.size();
    List<Integer> indicies = new Vector<Integer>(num_components);
    for (int i = 0; i < num_components; i++) {
      indicies.add(ArrayUtilityMethods.arrayContainsValueAtIndex(headers, path.get(i)));
    }

    // now that we have the indices
    // loop through the data
    // and construct everything
    for (Object[] dataRow : data) {
      // generate the map for the x axis
      generateParentChildMap(hierarchyMap, dataRow, indicies, num_components);
    }

    return hierarchyMap;
  }

  /**
   * This method is used to properly add values into the dendrograms that make up the 2 sides of the
   * clustergram visualization
   *
   * @param staring_map
   * @param dataRow
   * @param indices
   * @param num_components
   * @param NAME
   * @param PARENT
   * @param CHILDREN
   */
  private void generateParentChildMap(
      LinkedHashMap<Object, Object> staring_map,
      Object[] dataRow,
      List<Integer> indices,
      int num_components) {
    // we need to replace the reference with each iteration
    LinkedHashMap<Object, Object> subMap = staring_map;

    int counter = 0;
    while (counter < num_components) {
      LinkedHashSet childrenSet = (LinkedHashSet) subMap.get(CHILDREN);
      String parent = subMap.get(NAME).toString();

      int dataRowIndex = indices.get(counter);
      Object dataRowValue = dataRow[dataRowIndex];
      if (dataRowValue == null) {
        dataRowValue = NULL_PLACEHOLDER;
      }

      // see if child already exists
      LinkedHashMap childMap = getChildMap(dataRowValue, childrenSet);

      if (childMap == null) {
        // child doesn't exist
        // add him in
        childMap = new LinkedHashMap<Object, Object>();
        childMap.put(NAME, dataRowValue.toString());
        childMap.put(PARENT, parent);
        childMap.put(CHILDREN, new LinkedHashSet<Map<Object, Object>>());
        childrenSet.add(childMap);
      }
      // reset the x_sub map to point to this child
      subMap = childMap;

      // update the counter
      counter++;
    }
  }

  /**
   * Iterate through to find a child node if it exists
   *
   * @param name
   * @param childrenSet
   * @param NAME
   * @return
   */
  private LinkedHashMap<Object, Object> getChildMap(
      Object name, LinkedHashSet<LinkedHashMap> childrenSet) {
    for (LinkedHashMap childMap : childrenSet) {
      if (childMap.get(NAME).toString().equals(name.toString())) {
        return childMap;
      }
    }

    return null;
  }

  @Override
  public String getFormatType() {
    return "HIERARCHY";
  }
}
