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
package prerna.usertracking;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/** Shared query mechanics; each reader owns its authorization and allowed fields. */
final class AuditReadQuerySupport {
    private AuditReadQuerySupport() { }

    static void appendExactFilters(StringBuilder sql, List<Object> parameters,
            Map<String, String> allowedFields, Map<String, String> options, String lengthError) {
        for (var field : allowedFields.entrySet()) {
            String value = options.get(field.getKey());
            if (value != null && !value.isBlank()) {
                if (value.length() > 255) throw new IllegalArgumentException(lengthError);
                sql.append(" AND ").append(field.getValue()).append("=?");
                parameters.add(value);
            }
        }
    }

    static void appendDateFilters(StringBuilder sql, List<Object> parameters,
            Map<String, String> options, String dateError, String rangeError) {
        LocalDate from = date(options.get("from"), dateError);
        LocalDate to = date(options.get("to"), dateError);
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException(rangeError);
        }
        if (from != null) {
            sql.append(" AND EVENT_TIME>=?");
            parameters.add(Timestamp.valueOf(from.atStartOfDay()));
        }
        if (to != null) {
            sql.append(" AND EVENT_TIME<?");
            parameters.add(Timestamp.valueOf(to.plusDays(1).atStartOfDay()));
        }
    }

    static void bind(PreparedStatement statement, List<Object> parameters) throws SQLException {
        for (int i = 0; i < parameters.size(); i++) statement.setObject(i + 1, parameters.get(i));
    }

    static String utcTimestamp(Timestamp value) {
        return value == null ? null : value.toLocalDateTime().toInstant(ZoneOffset.UTC).toString();
    }

    private static LocalDate date(String value, String error) {
        if (value == null || value.isBlank()) return null;
        try {
            if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) return LocalDate.parse(value);
        } catch (java.time.DateTimeException ignored) { }
        throw new IllegalArgumentException(error);
    }
}

