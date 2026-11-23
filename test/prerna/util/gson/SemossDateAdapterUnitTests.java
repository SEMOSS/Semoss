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
package prerna.util.gson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;

import prerna.date.SemossDate;
import prerna.util.Constants;
import prerna.util.InternalUtility;

/**
 * Tests that every Gson writes a {@link SemossDate} through its adapter rather
 * than reflecting into its zone, and that each form it writes reads back as the
 * same date. Dates are in US Eastern time, EDT in March, while the
 * application's zone is UTC, so a date that lost its zone would show it.
 */
class SemossDateAdapterUnitTests {

	private static final ZoneId EASTERN = ZoneId.of("America/New_York");

	private static final String FULL_FORM = "{\"date\":\"15.03.2024 10:30\",\"pattern\":\"dd.MM.yyyy HH:mm\","
			+ "\"zone\":\"America/New_York\"}";

	private Properties savedCoreProp;

	@BeforeEach
	void useUtcAsTheApplicationZone() {
		savedCoreProp = InternalUtility.getCoreProp();
		Properties rdfMap = new Properties();
		rdfMap.setProperty(Constants.DEFAULT_TIME_ZONE, "UTC");
		InternalUtility.setCoreProp(rdfMap);
	}

	@AfterEach
	void restoreRdfMap() {
		InternalUtility.setCoreProp(savedCoreProp);
	}

	/**
	 * An object holding a date, which Gson would otherwise read and write by
	 * reflection.
	 */
	static class Holder {
		SemossDate when;
	}

	@Test
	void aPlainGsonWritesADateAsItsStringWithoutReflectingIntoItsZone() {
		Holder holder = new Holder();
		holder.when = new SemossDate("2024-03-15 10:30:00", "yyyy-MM-dd HH:mm:ss", EASTERN);

		String json = new Gson().toJson(holder);

		assertEquals("{\"when\":\"2024-03-15 10:30:00\"}", json);
		assertEquals("2024-03-15 10:30:00", new Gson().fromJson(json, Holder.class).when.getFormattedDate());
	}

	@Test
	void theFullFormKeepsThePatternAndZoneOfAnyDate() {
		Gson gson = new GsonBuilder().registerTypeAdapter(SemossDate.class, SemossDateAdapter.withPatternAndZone())
				.create();
		SemossDate date = new SemossDate("15.03.2024 10:30", "dd.MM.yyyy HH:mm", EASTERN);

		String json = gson.toJson(date);
		SemossDate read = gson.fromJson(json, SemossDate.class);

		assertEquals(FULL_FORM, json);
		assertEquals("dd.MM.yyyy HH:mm", read.getPattern());
		assertEquals(EASTERN, read.getZoneId());
		assertEquals(date.getZonedDateTime(), read.getZonedDateTime());
	}

	@Test
	void theDefaultAdapterReadsTheFullFormToo() {
		SemossDate read = new Gson().fromJson(FULL_FORM, SemossDate.class);

		assertEquals(EASTERN, read.getZoneId());
		assertEquals("15.03.2024 10:30", read.getFormattedDate());
	}

	@Test
	void readsAnIsoStringInTheZoneItNamesOnEitherSideOfDaylightSaving() {
		SemossDate edt = new Gson().fromJson("\"2024-03-15T10:30:00-04:00[America/New_York]\"", SemossDate.class);
		SemossDate est = new Gson().fromJson("\"2024-01-15T10:30:00-05:00[America/New_York]\"", SemossDate.class);

		assertEquals(EASTERN, edt.getZoneId());
		assertEquals(ZonedDateTime.of(2024, 3, 15, 10, 30, 0, 0, EASTERN), edt.getZonedDateTime());
		assertEquals(ZoneOffset.ofHours(-4), edt.getZonedDateTime().getOffset());
		assertEquals(EASTERN, est.getZoneId());
		assertEquals(ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, EASTERN), est.getZonedDateTime());
		assertEquals(ZoneOffset.ofHours(-5), est.getZonedDateTime().getOffset());
	}

	@Test
	void anObjectNamingAZoneThatDoesNotExistFails() {
		assertThrows(JsonParseException.class,
				() -> new Gson().fromJson(
						"{\"date\":\"2024-03-15\",\"pattern\":\"yyyy-MM-dd\",\"zone\":\"Mars/Olympus\"}",
						SemossDate.class));
	}

	@Test
	void aNullOrAStringThatIsNotADateReadsAsNull() {
		assertNull(new Gson().fromJson("null", SemossDate.class));
		assertNull(new Gson().fromJson("\"\"", SemossDate.class));
		assertNull(new Gson().fromJson("\"not a date\"", SemossDate.class));
	}

	@Test
	void aDateReadBackFromJavaSerializationStillFormats() throws Exception {
		SemossDate date = new SemossDate("2024-03-15", "yyyy-MM-dd", EASTERN);
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
			out.writeObject(date);
		}
		SemossDate copy;
		try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
			copy = (SemossDate) in.readObject();
		}

		assertEquals("2024-03-15", copy.getFormattedDate());
		assertEquals("\"2024-03-15\"", new Gson().toJson(copy));
	}
}
