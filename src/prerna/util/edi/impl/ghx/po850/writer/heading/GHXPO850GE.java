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
package prerna.util.edi.impl.ghx.po850.writer.heading;

import prerna.util.edi.IPO850GE;

public class GHXPO850GE implements IPO850GE {

  public String ge01 = "1"; // number of included ST segments
  public String ge02 = ""; // group control number - must match GS06

  @Override
  public String generateX12(String elementDelimiter, String segmentDelimiter) {

    String builder = "GE" + elementDelimiter + ge01 + elementDelimiter + ge02 + segmentDelimiter;

    return builder;
  }

  // setters/getter

  @Override
  public String getGe01() {
    return ge01;
  }

  @Override
  public GHXPO850GE setGe01(String ge01) {
    this.ge01 = ge01;
    return this;
  }

  @Override
  public GHXPO850GE setNumberOfTransactions(String ge01) {
    return setGe01(ge01);
  }

  @Override
  public String getGe02() {
    return ge02;
  }

  @Override
  public GHXPO850GE setGe02(String ge02) {
    if (ge02 == null || ge02.length() < 3) {
      throw new IllegalArgumentException("GS06 Group Control Number must be at least 3 digits");
    }
    this.ge02 = ge02;
    return this;
  }

  @Override
  public GHXPO850GE setGroupControlNumber(String ge02) {
    return setGe02(ge02);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    GHXPO850GE ge = new GHXPO850GE().setGroupControlNumber("001") // 02
        ;

    System.out.println(ge.generateX12("^", "~\n"));
  }
}
