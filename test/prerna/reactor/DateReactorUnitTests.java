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
package prerna.reactor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Calendar;
import java.util.Map;
import java.text.SimpleDateFormat;

import prerna.date.reactor.DateReactor;
import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateReactorUnitTests {

   /* private DateReactor reactor;
    private Map<String, String> keyValue;

    @BeforeEach
    public void setUp() {
        reactor = new DateReactor();
        keyValue = reactor.keyValue;
    }

    @Test
    public void getDate() {
        keyValue.put("date", "2022-03-19");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals(2022-03-19, date.getDate());
    }

    @Test
    public void getDate2() {
        //sanity check to make sure multiple tests run fine :)
    	keyValue.put("date", "2022-03-19");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals(2022-03-19, date.getDate());
    }

    @Test
    public void getDateWithCustomFormat() {
    	keyValue.put("date", "2022-03-19");
    	keyValue.put("format", "dd/MM/yyyy");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals(2022-03-19, date.getDate());

        assertEquals("19/03/2022", date);
    }

    @Test
    public void getDateWithNoInput() {
        NounMetadata nm = reactor.execute();
        String date = nm.getValue().toString();
        String today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());

        assertEquals(today, date);
    } */
}
