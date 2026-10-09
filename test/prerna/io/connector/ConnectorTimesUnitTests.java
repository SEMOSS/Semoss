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
package prerna.io.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

class ConnectorTimesUnitTests {

	private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

	@Test
	void readsALocalTimeInTheZoneNamedAndATimeWithAnOffsetAsGiven() {
		assertEquals(Instant.parse("2026-10-05T13:00:00Z"), ConnectorTimes.readMoment("2026-10-05T09:00:00", NEW_YORK));
		assertEquals(Instant.parse("2026-10-05T09:00:00Z"),
				ConnectorTimes.readMoment("2026-10-05T09:00:00Z", NEW_YORK));
		assertEquals(Instant.parse("2026-10-05T09:00:00Z"),
				ConnectorTimes.readMoment("2026-10-05T05:00:00-04:00", ZoneOffset.UTC));
		assertEquals(Instant.parse("2026-10-05T04:00:00Z"), ConnectorTimes.readMoment("2026-10-05", NEW_YORK));
	}

	@Test
	void readsAProviderTimeWithoutAnOffsetAsUtc() {
		assertEquals(Instant.parse("2026-10-05T13:00:00Z"),
				ConnectorTimes.parseProviderTime("2026-10-05T13:00:00.0000000"));
		assertEquals(Instant.parse("2026-10-05T13:00:00Z"),
				ConnectorTimes.parseProviderTime("2026-10-05T09:00:00-04:00"));
		assertEquals(null, ConnectorTimes.parseProviderTime("not a time"));
	}

	@Test
	void writesUtcToTheSecond() {
		assertEquals("2026-10-05T13:00:00Z", ConnectorTimes.format(Instant.parse("2026-10-05T13:00:00.123Z")));
	}

	@Test
	void takesOnlyIanaZonesAndIsoDates() {
		assertEquals(ZoneOffset.UTC, ConnectorTimes.readZone(null));
		assertThrows(IllegalArgumentException.class, () -> ConnectorTimes.readZone("Eastern Standard Time"));
		assertEquals(LocalDate.parse("2026-10-05"), ConnectorTimes.readDate("2026-10-05T23:00:00"));
		assertThrows(IllegalArgumentException.class, () -> ConnectorTimes.readMoment("tomorrow", ZoneOffset.UTC));
	}
}
