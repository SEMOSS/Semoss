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

public class PO850PID implements IX12Format {

  private String pid01 = "F";
  private String pid02 = "";
  private String pid03 = "";
  private String pid04 = "";
  private String pid05 = "";

  @Override
  public String generateX12(String elementDelimiter, String segmentDelimiter) {

    String builder =
        "PID"
            + elementDelimiter
            + pid01
            + elementDelimiter
            + pid02
            + elementDelimiter
            + pid03
            + elementDelimiter
            + pid04
            + elementDelimiter
            + pid05
            + segmentDelimiter;

    return builder;
  }

  // setters/getter

  public String getPid01() {
    return pid01;
  }

  public PO850PID setPid01(String pid01) {
    this.pid01 = pid01;
    return this;
  }

  public PO850PID setItemDescriptionType(String pid01) {
    this.pid01 = pid01;
    return this;
  }

  //	public String getPid02() {
  //		return pid02;
  //	}
  //
  //	public PO850PID setPid02(String pid02) {
  //		this.pid02 = pid02;
  //		return this;
  //	}
  //
  //	public String getPid03() {
  //		return pid03;
  //	}
  //
  //	public PO850PID setPid03(String pid03) {
  //		this.pid03 = pid03;
  //		return this;
  //	}
  //
  //	public String getPid04() {
  //		return pid04;
  //	}
  //
  //	public PO850PID setPid04(String pid04) {
  //		this.pid04 = pid04;
  //		return this;
  //	}

  public String getPid05() {
    return pid05;
  }

  public PO850PID setPid05(String pid05) {
    this.pid05 = pid05;
    if (this.pid05.length() > 80) {
      this.pid05 = this.pid05.substring(0, 80);
    }
    return this;
  }

  public PO850PID setItemDescription(String pid05) {
    return setPid05(pid05);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    PO850PID pid =
        new PO850PID()
            .setItemDescription(
                "CARDINAL HEALTH™ WOUND CLOSURE STRIP, REINFORCED, 0.125 X 3IN, FOB (Destination), Manufacturer (CARDINAL HEALTH 200, LLC); BOX of 50");

    System.out.println(pid.generateX12("^", "~\n"));
  }
}
