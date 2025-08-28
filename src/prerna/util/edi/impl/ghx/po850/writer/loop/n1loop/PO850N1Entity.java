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

public class PO850N1Entity implements IX12Format {

  private PO850N1 n1;
  private PO850N2 n2;
  private PO850N3 n3;
  private PO850N4 n4;

  @Override
  public String generateX12(String elementDelimiter, String segmentDelimiter) {
    String builder = n1.generateX12(elementDelimiter, segmentDelimiter);
    if (n2 != null) {
      builder += n2.generateX12(elementDelimiter, segmentDelimiter);
    }
    if (n3 != null) {
      builder += n3.generateX12(elementDelimiter, segmentDelimiter);
    }
    if (n4 != null) {
      builder += n4.generateX12(elementDelimiter, segmentDelimiter);
    }
    return builder;
  }

  public PO850N1 getN1() {
    return n1;
  }

  public PO850N1Entity setN1(PO850N1 n1) {
    this.n1 = n1;
    return this;
  }

  public PO850N2 getN2() {
    return n2;
  }

  public PO850N1Entity setN2(PO850N2 n2) {
    this.n2 = n2;
    return this;
  }

  public PO850N3 getN3() {
    return n3;
  }

  public PO850N1Entity setN3(PO850N3 n3) {
    this.n3 = n3;
    return this;
  }

  public PO850N4 getN4() {
    return n4;
  }

  public PO850N1Entity setN4(PO850N4 n4) {
    this.n4 = n4;
    return this;
  }

  public int getNumSegments() {
    int counter = 0;
    counter = n1 != null ? counter + 1 : counter;
    counter = n2 != null ? counter + 1 : counter;
    counter = n3 != null ? counter + 1 : counter;
    counter = n4 != null ? counter + 1 : counter;

    return counter;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    PO850N1Entity n1group =
        new PO850N1Entity()
            .setN1(
                new PO850N1()
                    .setEntityCode("ST") // 1 - ship to
                    .setName("Anchorage VA Medical Center") // 2 - name
                    .setIdentificationCode("91") // 3 - 91=assigned by seller
                    .setIdentificationCode("DEMO-ID"))
            .setN3(new PO850N3().setAddressInfo1("1201 N Muldoon Rd"))
            .setN4(
                new PO850N4()
                    .setCity("Anchorage")
                    .setState("AK")
                    .setZip("99504")
                    .setCountryCode("US"));

    System.out.println(n1group.generateX12("^", "~\n"));
  }
}
