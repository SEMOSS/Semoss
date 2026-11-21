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

import java.io.IOException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.date.SemossDate;
import prerna.util.Utility;

/**
 * Writes and reads a {@link SemossDate} as JSON. {@link SemossDate} names this
 * adapter in its {@link JsonAdapter} annotation, so every Gson uses it and none
 * reflects into the date's {@link ZoneId}, which Java does not allow.
 *
 * <p>
 * A date is written as its formatted string, such as
 * {@code 2024-03-15 10:30:00}, the form pixel results and REST responses carry.
 * The adapter from {@link #withPatternAndZone()} writes it as an object that
 * keeps its pattern and zone, for JSON that only SEMOSS reads back:
 *
 * <pre>
 * {"date": "2024-03-15 10:30:00", "pattern": "yyyy-MM-dd HH:mm:ss", "zone": "America/New_York"}
 * </pre>
 *
 * <p>
 * Reading takes either form. An object becomes the date it was written from,
 * with the same pattern and zone. A string with an offset or zone, in ISO-8601
 * such as {@code 2024-03-15T10:30:00-04:00}, is read in the zone it names; any
 * other string is read with the date and timestamp patterns {@link SemossDate}
 * knows, in the application's zone.
 */
public class SemossDateAdapter extends TypeAdapter<SemossDate> {

	private static final Logger classLogger = LogManager.getLogger(SemossDateAdapter.class);

	private static final String DATE = "date";
	private static final String PATTERN = "pattern";
	private static final String ZONE = "zone";

	private final boolean writesPatternAndZone;

	/**
	 * An adapter that writes each date as its formatted string. Gson builds this
	 * one from the annotation on {@link SemossDate}.
	 */
	public SemossDateAdapter() {
		this(false);
	}

	/**
	 * An adapter that writes each date as its formatted string, or as an object
	 * keeping its pattern and zone.
	 *
	 * @param writesPatternAndZone whether to write the object
	 */
	private SemossDateAdapter(boolean writesPatternAndZone) {
		this.writesPatternAndZone = writesPatternAndZone;
	}

	/**
	 * An adapter that writes each date as an object keeping its pattern and zone,
	 * so it reads back as exactly the same date. Register it on a Gson whose JSON
	 * only SEMOSS reads back, such as saved state, with
	 * {@code registerTypeAdapter(SemossDate.class, SemossDateAdapter.withPatternAndZone())}.
	 *
	 * @return the adapter
	 */
	public static SemossDateAdapter withPatternAndZone() {
		return new SemossDateAdapter(true);
	}

	/**
	 * Writes a date as its formatted string, or as an object keeping its pattern
	 * and zone. A null or invalid date is written as null.
	 *
	 * @param out   the JSON being written
	 * @param value the date
	 * @throws IOException if the JSON cannot be written
	 */
	@Override
	public void write(JsonWriter out, SemossDate value) throws IOException {
		String date = value == null ? null : value.getFormattedDate();
		if (date == null) {
			out.nullValue();
			return;
		}
		if (!writesPatternAndZone) {
			out.value(date);
			return;
		}
		out.beginObject();
		out.name(DATE).value(date);
		out.name(PATTERN).value(value.getPattern());
		out.name(ZONE).value(value.getZoneId().getId());
		out.endObject();
	}

	/**
	 * Reads a date written as a string or as an object keeping its pattern and
	 * zone.
	 *
	 * @param in the JSON being read
	 * @return the date, or null for a null or a string that is not a date
	 * @throws IOException        if the JSON cannot be read
	 * @throws JsonParseException if an object names a zone that does not exist
	 */
	@Override
	public SemossDate read(JsonReader in) throws IOException {
		JsonToken token = in.peek();
		if (token == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		if (token == JsonToken.BEGIN_OBJECT) {
			return readObject(in);
		}
		return readString(in.nextString(), null);
	}

	/**
	 * Reads a date written as an object with its pattern and zone, as the same
	 * date. An object without a pattern, or whose date does not match it, is read
	 * as a string in its zone.
	 *
	 * @param in the JSON being read, at the start of the object
	 * @return the date, or null when the object holds none
	 * @throws IOException        if the JSON cannot be read
	 * @throws JsonParseException if the object names a zone that does not exist
	 */
	private static SemossDate readObject(JsonReader in) throws IOException {
		String date = null;
		String pattern = null;
		String zone = null;
		in.beginObject();
		while (in.hasNext()) {
			String name = in.nextName();
			if (in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			switch (name) {
			case DATE -> date = in.nextString();
			case PATTERN -> pattern = in.nextString();
			case ZONE -> zone = in.nextString();
			default -> in.skipValue();
			}
		}
		in.endObject();
		if (date == null) {
			return null;
		}
		ZoneId zoneId = readZone(zone, in);
		if (pattern != null) {
			SemossDate parsed = new SemossDate(date, pattern, zoneId);
			if (parsed.getZonedDateTime() != null) {
				return parsed;
			}
		}
		return readString(date, zoneId);
	}

	/**
	 * The zone an object names.
	 *
	 * @param zone the zone's id, or null when the object names none
	 * @param in   the JSON being read, for the error's location
	 * @return the zone, or null for none
	 * @throws JsonParseException if the zone does not exist
	 */
	private static ZoneId readZone(String zone, JsonReader in) {
		if (zone == null) {
			return null;
		}
		try {
			return ZoneId.of(zone);
		} catch (DateTimeException e) {
			throw new JsonParseException(
					"The date at " + in.getPath() + " names the zone '" + zone + "', which does not exist", e);
		}
	}

	/**
	 * Reads a date from its string: as ISO-8601 with an offset or zone, in the zone
	 * it names, and otherwise with the date and then the timestamp patterns
	 * {@link SemossDate} knows, in the given zone.
	 *
	 * @param text   the string
	 * @param zoneId the zone for a string that names none, or null for the
	 *               application's zone
	 * @return the date, or null when the string is blank or not a date
	 */
	private static SemossDate readString(String text, ZoneId zoneId) {
		if (text.isBlank()) {
			return null;
		}
		SemossDate date = readIso(text);
		if (date == null) {
			date = zoneId == null ? SemossDate.genDateObj(text) : SemossDate.genDateObj(text, zoneId);
		}
		if (date == null) {
			date = zoneId == null ? SemossDate.genTimeStampDateObj(text) : SemossDate.genTimeStampDateObj(text, zoneId);
		}
		if (date == null) {
			classLogger.warn("Read '{}' as no date, since it is not in a date format SemossDate reads",
					Utility.cleanLogString(text));
		}
		return date;
	}

	/**
	 * Reads an ISO-8601 date and time with an offset or zone, such as
	 * {@code 2024-03-15T10:30:00Z} or
	 * {@code 2024-03-15T10:30:00-04:00[America/New_York]}.
	 *
	 * @param text the string
	 * @return the date in the zone it names, or null when it is not one
	 */
	private static SemossDate readIso(String text) {
		try {
			return new SemossDate(ZonedDateTime.parse(text, DateTimeFormatter.ISO_ZONED_DATE_TIME));
		} catch (DateTimeParseException e) {
			return null;
		}
	}
}
