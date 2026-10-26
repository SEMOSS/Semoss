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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.engine.api.IRDBMSEngine;

class SystemEngineRegistryUnitTests {

	static Stream<Arguments> databases() {
		return Stream.of(Arguments.of(Constants.SECURITY_DB, "securityDbHolder"),
				Arguments.of(Constants.LOCAL_MASTER_DB, "localMasterDbHolder"),
				Arguments.of(Constants.SCHEDULER_DB, "schedulerDbHolder"),
				Arguments.of(Constants.THEMING_DB, "themesDbHolder"),
				Arguments.of(Constants.USER_TRACKING_DB, "userTrackingDbHolder"),
				Arguments.of(Constants.PROMPT_DB, "promptDbHolder"),
				Arguments.of(Constants.NOTIFICATION_DB, "notificationDbHolder"),
				Arguments.of(Constants.AUDIT_LOGS_DB, "auditLogsDbHolder"),
				Arguments.of(Constants.MODEL_INFERENCE_LOGS_DB, "modelInferenceLogsDbHolder"));
	}

	@ParameterizedTest
	@MethodSource("databases")
	void checksEverySystemDatabaseWithoutRetrievingItsEngine(String engineId, String holderName) throws Exception {
		Field holder = holder(holderName);
		Object previous = holder.get(null);
		try {
			holder.set(null, null);
			assertEquals("System database '" + engineId + "' is required for this operation",
					assertThrows(IllegalArgumentException.class, () -> SystemEngineRegistry.requireDatabase(engineId))
							.getMessage());
			assertEquals("System database '" + engineId + "' is required for agent import",
					assertThrows(IllegalArgumentException.class,
							() -> SystemEngineRegistry.requireDatabase(engineId, "  agent import  ")).getMessage());
			holder.set(null, (Supplier<IRDBMSEngine>) () -> {
				throw new AssertionError("A loaded-state check must not retrieve a guarded engine");
			});
			assertDoesNotThrow(() -> SystemEngineRegistry.requireDatabase(engineId));
			assertDoesNotThrow(() -> SystemEngineRegistry.requireDatabase(engineId, "agent import"));
		} finally {
			holder.set(null, previous);
		}
	}

	@ParameterizedTest
	@NullAndEmptySource
	@ValueSource(strings = { " ", "\t\n" })
	void defaultsMissingOperationText(String operation) throws Exception {
		Field holder = holder("modelInferenceLogsDbHolder");
		Object previous = holder.get(null);
		try {
			holder.set(null, null);
			assertEquals("System database 'ModelInferenceLogsDatabase' is required for this operation",
					assertThrows(IllegalArgumentException.class,
							() -> SystemEngineRegistry.requireDatabase(Constants.MODEL_INFERENCE_LOGS_DB, operation))
							.getMessage());
		} finally {
			holder.set(null, previous);
		}
	}

	@Test
	void rejectsUnknownDatabaseIds() {
		assertEquals("Unknown system database: custom-model",
				assertThrows(IllegalArgumentException.class, () -> SystemEngineRegistry.requireDatabase("custom-model"))
						.getMessage());
	}

	@Test
	void rejectsMissingDatabaseIds() {
		assertEquals("System database id is required",
				assertThrows(IllegalArgumentException.class, () -> SystemEngineRegistry.requireDatabase(null))
						.getMessage());
	}

	private Field holder(String name) throws ReflectiveOperationException {
		Field field = SystemEngineRegistry.class.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}
}
