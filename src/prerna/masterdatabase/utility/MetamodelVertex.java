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
package prerna.masterdatabase.utility;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** class to simplify the implementation of combining alias names and properties on vertices */
public class MetamodelVertex {

  // store the property conceptual names
  private Set<String> propSet = new TreeSet<String>();
  // the conceptual name for the concept
  private String conceptualName;

  public MetamodelVertex(String conceptualName) {
    this.conceptualName = conceptualName;
  }

  /**
   * Add to the properties for the vertex
   *
   * @param propertyConceptual
   * @param propertyAlias
   */
  public void addProperty(String propertyConceptual) {
    if (propertyConceptual.equals("noprop")) {
      return;
    }
    propSet.add(propertyConceptual);
  }

  public Map<String, Object> toMap() {
    Map<String, Object> vertexMap = new Hashtable<String, Object>();
    vertexMap.put("conceptualName", this.conceptualName);
    vertexMap.put("propSet", this.propSet);
    return vertexMap;
  }

  public String toString() {
    return toMap().toString();
  }

  public String getConceptualName() {
    return this.conceptualName;
  }

  public Set<String> getPropSet() {
    return this.propSet;
  }
}
