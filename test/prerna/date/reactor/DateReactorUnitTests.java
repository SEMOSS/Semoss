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
package prerna.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;
import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateReactorUnitTests {
  DateReactor reactor;

  @Test
  void test() {
    reactor = new DateReactor();
    reactor.keyValue.put("date", "2025-01-01");
    reactor.keyValue.put("format", "yyyy-MM-dd");

    NounMetadata nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());

    assertEquals(
        "Get todays date or return a date based on a specific date input and format",
        reactor.getReactorDescription());
    assertEquals(
        "A specific date to return. This is a string and assumes a date of yyyy-MM-dd",
        reactor.getDescriptionForKey("date"));
    assertEquals(
        "A specified format for the date parameter to parse. This should be a Java compliant format",
        reactor.getDescriptionForKey("format"));
    assertEquals(
        "A default value to use for null columns", reactor.getDescriptionForKey("defaultValue"));

    reactor.keyValue.remove("date");
    nm = reactor.execute();
    assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
    assertInstanceOf(SemossDate.class, nm.getValue());
  }
}
