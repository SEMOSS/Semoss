/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.sablecc2.om;

import java.util.Iterator;

public interface IOutputRow extends Iterator {

  // couple of things needed
  // need a json function to push the output
  // need a way to rip and replace columns
  // need a way add a column on the fly
  // I need to indicate to the output row that I am changing the column definitions

  // this call is to convert the objects into vectors
  // convert structures to vectors
  // make the headers to cardinality
  public void open();

  public String toJson();

  // add a column
  public void add(String columnName, Object value);

  // just add a value instead of column name and shit
  public void add(Object value);

  // get a particular column
  public Object get(String columnName);

  // add a column to the overall scheme
  public void add(String columnName);
}
