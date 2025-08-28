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
package prerna.testing.utility;

import java.time.LocalDateTime;

public class TestExcelInputObject {

  private boolean isNull = false;
  private Integer i;
  private String s;
  private Double d;
  private LocalDateTime ldt;
  private Boolean b;
  private TestExcelType type;

  public TestExcelInputObject() {}

  public boolean isNull() {
    return isNull;
  }

  public void setNull(boolean isNull) {
    this.isNull = isNull;
  }

  public Integer getI() {
    return i;
  }

  public void setI(Integer i) {
    this.i = i;
  }

  public String getS() {
    return s;
  }

  public void setS(String s) {
    this.s = s;
  }

  public Double getD() {
    return d;
  }

  public void setD(Double d) {
    this.d = d;
  }

  public LocalDateTime getLdt() {
    return ldt;
  }

  public void setLdt(LocalDateTime ldt) {
    this.ldt = ldt;
  }

  public Boolean getB() {
    return b;
  }

  public void setB(Boolean b) {
    this.b = b;
  }

  public TestExcelType getType() {
    return type;
  }

  public void setType(TestExcelType type) {
    this.type = type;
  }
}
