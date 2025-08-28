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
package prerna.util.edi.impl.ghx.po850.writer.loop.n1loop;

import java.util.ArrayList;
import java.util.List;
import prerna.util.edi.IX12Format;

public class PO850N1Loop implements IX12Format {

  private List<PO850N1Entity> n1list = new ArrayList<>();

  @Override
  public String generateX12(String elementDelimiter, String segmentDelimiter) {
    String builder = "";
    for (PO850N1Entity loop : n1list) {
      builder += loop.generateX12(elementDelimiter, segmentDelimiter);
    }
    return builder;
  }

  public PO850N1Loop addN1Group(PO850N1Entity n1) {
    n1list.add(n1);
    return this;
  }

  public List<PO850N1Entity> getN1List() {
    return this.n1list;
  }

  public int getNumSegments() {
    int counter = 0;
    for (PO850N1Entity loop : n1list) {
      counter += loop.getNumSegments();
    }
    return counter;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    PO850N1Loop n1loop =
        new PO850N1Loop()
            .addN1Group(
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
                            .setCountryCode("US")));

    System.out.println(n1loop.generateX12("^", "~\n"));
  }
}
