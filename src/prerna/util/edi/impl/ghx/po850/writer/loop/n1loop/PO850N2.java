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
package prerna.util.edi.impl.ghx.po850.writer.loop.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N2 implements IX12Format {

  private String n201 = ""; // address information name
  private String n202 = ""; // address information name

  @Override
  public String generateX12(String elementDelimiter, String segmentDelimiter) {

    String builder = "N2" + elementDelimiter + n201 + elementDelimiter + n202 + segmentDelimiter;

    return builder;
  }

  // setters/getter

  public String getN201() {
    return n201;
  }

  public PO850N2 setN201(String n201) {
    this.n201 = n201;
    return this;
  }

  public String getN202() {
    return n202;
  }

  public PO850N2 setN202(String n202) {
    this.n202 = n202;
    return this;
  }
}
