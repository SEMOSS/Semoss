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
package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IPO850ST;

public class GHXPO850ST implements IPO850ST {

  private String st01 = "850";
  private String st02 = ""; // transaction control number - must match se02

  @Override
  public String generateX12(String elementDelimiter, String segmentDelimiter) {
    String builder = "ST" + elementDelimiter + st01 + elementDelimiter + st02 + segmentDelimiter;

    return builder;
  }

  // setters/getter

  @Override
  public String getSt02() {
    return st02;
  }

  @Override
  public GHXPO850ST setSt02(String st02) {
    if (st02 == null || st02.length() < 4 || st02.length() > 9) {
      throw new IllegalArgumentException(
          "ST02 transaction control number must be between 4 and 9 digits long");
    }
    this.st02 = st02;
    return this;
  }

  @Override
  public GHXPO850ST setTransactionSetControlNumber(String st02) {
    return setSt02(st02);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    GHXPO850ST st = new GHXPO850ST().setTransactionSetControlNumber("0001") // 2
        ;

    System.out.println(st.generateX12("^", "~\n"));
  }
}
