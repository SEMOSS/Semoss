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
package prerna.util.edi.impl.ghx.po850.writer.loop.po1loop;

import prerna.util.edi.IX12Format;
import prerna.util.edi.po850.enums.PO850PO1QualifierIdEnum;

public class PO850PO1Entity implements IX12Format {

  private PO850PO1 po1;
  private PO850PID pid;

  @Override
  public String generateX12(String elementDelimiter, String segmentDelimiter) {
    String builder = po1.generateX12(elementDelimiter, segmentDelimiter);
    if (pid != null) {
      builder += pid.generateX12(elementDelimiter, segmentDelimiter);
    }

    return builder;
  }

  public PO850PO1 getPo1() {
    return po1;
  }

  public PO850PO1Entity setPo1(PO850PO1 po1) {
    this.po1 = po1;
    return this;
  }

  public PO850PID getPid() {
    return pid;
  }

  public PO850PO1Entity setPid(PO850PID pid) {
    this.pid = pid;
    return this;
  }

  public int getNumSegments() {
    int counter = 0;
    counter = po1 != null ? counter + 1 : counter;
    counter = pid != null ? counter + 1 : counter;

    return counter;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    PO850PO1Entity po1group =
        new PO850PO1Entity()
            .setPo1(
                new PO850PO1()
                    .setUniqueId("1") // 1 - unique id
                    .setQuantityOrdered(10) // 2 - quantity ordered
                    .setUnitOfMeasure("BX") // 3 - unit measurement
                    .setUnitPrice(27.50) // 4 unit price (not total)
                    .addQualifierAndValue(PO850PO1QualifierIdEnum.VC, "BXTS1040")
                    .addQualifierAndValue(PO850PO1QualifierIdEnum.IN, "299176"))
            .setPid(
                new PO850PID()
                    .setItemDescription(
                        "CARDINAL HEALTH™ WOUND CLOSURE STRIP, REINFORCED, 0.125 X 3IN, FOB (Destination), Manufacturer (CARDINAL HEALTH 200, LLC); BOX of 50"));

    System.out.println(po1group.generateX12("^", "~\n"));
  }
}
