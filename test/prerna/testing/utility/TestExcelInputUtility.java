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

public class TestExcelInputUtility {

  public static TestExcelInputObject getString(String s) {
    TestExcelInputObject o = new TestExcelInputObject();
    o.setS(s);
    o.setType(TestExcelType.STRING);
    return o;
  }

  public static TestExcelInputObject getInteger(int i) {
    TestExcelInputObject o = new TestExcelInputObject();
    o.setI(i);
    o.setType(TestExcelType.INTEGER);
    return o;
  }

  public static TestExcelInputObject getDate(LocalDateTime ldt) {
    TestExcelInputObject o = new TestExcelInputObject();
    o.setLdt(ldt);
    o.setType(TestExcelType.DATE);
    return o;
  }

  public static TestExcelInputObject getDouble(double d) {
    TestExcelInputObject o = new TestExcelInputObject();
    o.setD(d);
    o.setType(TestExcelType.DOUBLE);
    return o;
  }

  public static TestExcelInputObject getBoolean(boolean b) {
    TestExcelInputObject o = new TestExcelInputObject();
    o.setB(b);
    o.setType(TestExcelType.BOOLEAN);
    return o;
  }

  public static TestExcelInputObject isNull(boolean isNull) {
    TestExcelInputObject o = new TestExcelInputObject();
    o.setNull(isNull);
    o.setType(TestExcelType.NULL);
    return o;
  }

  // public static TestExcelInputObject getString(String s) {
  // 	TestExcelInputObject o = new TestExcelInputObject();
  // 	o.setS(s);
  // 	o.setType(TestExcelType.STRING);
  // 	return o;
  // }
}
