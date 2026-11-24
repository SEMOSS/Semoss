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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.ObjectMessage;
import org.junit.jupiter.api.Test;

/**
 * What an Activity Log row keeps of an engine call the proxy logged.
 */
class AuditLogsJDBCAppenderUnitTests {

	@Test
	void aRowKeepsWhyTheCallWasAllowedAndTheDetails() throws Exception {
		AuditLogsJDBCAppender appender = AuditLogsJDBCAppender.createAppender("audit-logs-test", null, null, true,
				"audit-logs-test-db", 10, 1_000, 100, 1);
		Map<String, Object> logged = new LinkedHashMap<>();
		logged.put(SemossLogUtils.IS_SUCCESS, true);
		logged.put(SemossLogUtils.ACCESS_REASON, "DELEGATED");
		logged.put(SemossLogUtils.ACCESS_DETAILS, "{\"level\":\"READ_ONLY\",\"granteeId\":\"orders-agent\"}");
		LogEvent event = Log4jLogEvent.newBuilder().setLoggerName("engine-test").setLevel(Level.INFO)
				.setMessage(new ObjectMessage(logged)).build();

		List<Object> values = Arrays.asList(appender.columnValues(event));

		assertTrue(values.contains("DELEGATED"), values.toString());
		assertTrue(values.contains("{\"level\":\"READ_ONLY\",\"granteeId\":\"orders-agent\"}"), values.toString());
	}

}
