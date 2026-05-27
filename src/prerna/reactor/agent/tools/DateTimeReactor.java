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
package prerna.reactor.agent.tools;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Returns the current date and time in one or more standard formats.
 *
 * <p>Agents frequently need an absolute date — for example, to convert "Thursday" to a real date
 * before saving a project memory, to stamp a commit message, or to compute a deadline. In
 * Claude Code this is available implicitly via the environment context; this reactor gives
 * SEMOSS agents the same capability without shelling out to {@code date}.
 *
 * <p>Optional arguments:
 * <ul>
 *   <li>{@code timezone} — IANA zone (e.g. {@code America/New_York}, {@code UTC}); defaults to
 *       the JVM default</li>
 *   <li>{@code format} — custom {@link DateTimeFormatter} pattern; if omitted the response
 *       includes ISO-8601 instant, local date-time in the chosen zone, and unix epoch seconds</li>
 * </ul>
 */
public class DateTimeReactor extends AbstractAgentToolReactor {

    public DateTimeReactor() {
        this.keysToGet   = new String[] { "timezone", "format" };
        this.keyRequired = new int[]    { 0,          0        };
    }

    @Override
    protected NounMetadata doExecute() throws Exception {
        String zoneIn = this.keyValue.get("timezone");
        String format = this.keyValue.get("format");

        ZoneId zone;
        try {
            zone = (zoneIn == null || zoneIn.trim().isEmpty())
                    ? ZoneId.systemDefault()
                    : ZoneId.of(zoneIn.trim());
        } catch (Exception e) {
            return new NounMetadata(
                    "Error: invalid timezone: " + zoneIn + " (" + e.getMessage() + ")",
                    PixelDataType.CONST_STRING);
        }

        Instant now = Instant.now();
        LocalDateTime local = LocalDateTime.ofInstant(now, zone);

        if (format != null && !format.trim().isEmpty()) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.trim());
                return new NounMetadata(local.format(formatter), PixelDataType.CONST_STRING);
            } catch (IllegalArgumentException e) {
                return new NounMetadata(
                        "Error: invalid format pattern: " + e.getMessage(),
                        PixelDataType.CONST_STRING);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("instant_iso: ").append(DateTimeFormatter.ISO_INSTANT.format(now)).append('\n');
        sb.append("local_iso: ").append(local.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
          .append('\n');
        sb.append("timezone: ").append(zone.getId()).append('\n');
        sb.append("epoch_seconds: ").append(now.getEpochSecond()).append('\n');
        sb.append("epoch_millis: ").append(now.toEpochMilli());
        return new NounMetadata(sb.toString(), PixelDataType.CONST_STRING);
    }

    @Override
    protected String getDescriptionForKey(String key) {
        switch (key) {
            case "timezone": return "IANA timezone (e.g. America/New_York, UTC). Defaults to the JVM default zone.";
            case "format":   return "DateTimeFormatter pattern (e.g. yyyy-MM-dd HH:mm:ss). "
                                  + "Omit to receive ISO instant, local ISO, zone id, epoch seconds, and epoch millis.";
            default:         return super.getDescriptionForKey(key);
        }
    }

    @Override
    public String getReactorDescription() {
        return "Returns the current date and time. Optional timezone (IANA zone) and format "
             + "(DateTimeFormatter pattern) arguments. Without format, returns ISO instant, local "
             + "ISO, zone, and epoch seconds/millis.";
    }
}
