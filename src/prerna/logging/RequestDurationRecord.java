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
package prerna.logging;

import java.sql.Timestamp;

/**
 * Request-level timing aggregate derived from {@code AUDIT_LOGS} rows that
 * share the same {@code REQUEST_ID}. Computed by
 * {@link prerna.engine.logging.AuditLogsDbUtils} for the request ids in the
 * current page and joined into each {@link LogActivityRecord} returned to the
 * timeline view.
 *
 * @param startTime       Earliest {@code REQUEST_START_TIME} observed for the
 *                        request ({@code MIN(REQUEST_START_TIME)}).
 * @param endTime         Latest {@code RESPONSE_END_TIME} observed for the
 *                        request ({@code MAX(RESPONSE_END_TIME)}).
 * @param durationSeconds Wall-clock latency between {@code startTime} and
 *                        {@code endTime}, in seconds
 *                        ({@code DATEDIFF(SECOND, MIN(...), MAX(...))}).
 */
public record RequestDurationRecord(Timestamp startTime, Timestamp endTime, long durationSeconds) {
}
