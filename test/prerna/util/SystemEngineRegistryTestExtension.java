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

import java.lang.reflect.Field;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import prerna.engine.api.IRDBMSEngine;

/**
 * JUnit 5 extension that clears all system engine registrations in
 * {@link SystemEngineRegistry} after each test class completes. This allows
 * each test class to register its own fresh databases without hitting "already
 * registered".
 *
 * Lives in the test source tree only - not shipped in production JARs. Uses
 * reflection to null the private holder fields so that no test-related code
 * needs to exist in the production class.
 *
 * Also provides static helpers used by the integration test bootstrap
 * ({@code ApiSemossTestEngineUtils}) to load and reset engines:
 * <ul>
 * <li>{@link #loadForTesting(String)} - delegates to
 * {@link SystemEngineRegistry#loadSystemEngine(String)} from within
 * {@code prerna.util}, which is on the registration allowlist.</li>
 * <li>{@link #resetAll()} - closes and nulls all nine engine holders.</li>
 * </ul>
 */
public class SystemEngineRegistryTestExtension implements AfterAllCallback {

	private static final Logger classLogger = LogManager.getLogger(SystemEngineRegistryTestExtension.class);

	private static final String[] HOLDER_FIELDS = { "securityDbHolder", "localMasterDbHolder", "schedulerDbHolder",
			"themesDbHolder", "userTrackingDbHolder", "promptDbHolder", "notificationDbHolder", "auditLogsDbHolder",
			"modelInferenceLogsDbHolder" };

	@Override
	public void afterAll(ExtensionContext context) throws Exception {
		resetAll();
	}

	/**
	 * Loads a system engine from its SMSS file and registers it in
	 * {@link SystemEngineRegistry}. Delegates to
	 * {@link SystemEngineRegistry#loadSystemEngine(String)} from within
	 * {@code prerna.util}, which is on the registration allowlist.
	 *
	 * @param smssFilePath absolute path to the engine's .smss file
	 * @return the opened and registered engine
	 * @throws Exception if loading or opening fails
	 */
	public static IRDBMSEngine loadForTesting(String smssFilePath) throws Exception {
		return SystemEngineRegistry.loadSystemEngine(smssFilePath);
	}

	/**
	 * Closes all registered engines and resets every holder to {@code null} so that
	 * a subsequent test suite can re-initialize without hitting the one-time
	 * registration guard. Closing releases any file locks held by the underlying
	 * DB.
	 */
	@SuppressWarnings("unchecked")
	public static void resetAll() {
		for (String fieldName : HOLDER_FIELDS) {
			try {
				Field field = SystemEngineRegistry.class.getDeclaredField(fieldName);
				field.setAccessible(true);
				java.util.function.Supplier<IRDBMSEngine> holder = (java.util.function.Supplier<IRDBMSEngine>) field
						.get(null);
				if (holder != null) {
					try {
						IRDBMSEngine engine = holder.get();
						if (engine != null) {
							engine.close();
						}
					} catch (Exception e) {
						classLogger.warn("Error closing engine '{}' during registry reset", fieldName, e);
					}
					field.set(null, null);
				}
			} catch (Exception e) {
				classLogger.warn("Could not reset registry field '{}'", fieldName, e);
			}
		}
	}

}
