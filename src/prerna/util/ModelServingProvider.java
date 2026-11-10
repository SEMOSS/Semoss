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

import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;

/**
 * Who hosts a model engine, as the lowercase key the model and built-in tools
 * catalogs use ({@link StaticBuiltinToolsCatalog#normalizeProviderKey}): the
 * saved {@code MODELMETADATA.SERVINGPROVIDER} when it is set; otherwise the
 * smss {@code PROVIDER} variable, then the smss {@code MODEL_TYPE}, which names
 * the client implementation and so tracks the host. Built-in tools and cost
 * limits both read it here, so they agree on who serves a model.
 */
public final class ModelServingProvider {

	private static final Logger classLogger = LogManager.getLogger(ModelServingProvider.class);

	/** The smss variable some engines name their host with, such as google */
	public static final String SMSS_PROVIDER = "PROVIDER";

	private ModelServingProvider() {

	}

	/**
	 * Works out who hosts a model engine, in the order the class javadoc lists. The
	 * smss file is read only when the {@code MODELMETADATA} row does not say.
	 *
	 * @param engineId the model engine
	 * @param metadata its {@code MODELMETADATA} row, or null
	 * @return who hosts it, or null when neither the row nor the smss says
	 */
	public static String resolve(String engineId, Map<String, Object> metadata) {
		// the saved value wins, and reading the smss is a file read, so it is only
		// read when the row does not say
		String saved = fromMetadata(metadata);
		if (saved != null) {
			return saved;
		}
		return resolve(metadata, loadSmssProperties(engineId));
	}

	/**
	 * For a caller that has the smss properties already, such as the built-in tools
	 * reactor, which reads the model id from them too.
	 *
	 * @param metadata the engine's {@code MODELMETADATA} row, or null
	 * @param smssProp the engine's smss properties, or null
	 * @return who hosts it, or null when neither says
	 */
	public static String resolve(Map<String, Object> metadata, Properties smssProp) {
		String servingProvider = fromMetadata(metadata);
		if (servingProvider == null && smssProp != null) {
			servingProvider = StaticBuiltinToolsCatalog
					.normalizeProviderKey(ValueUtils.trimToNull(smssProp.getProperty(SMSS_PROVIDER)));
		}
		// MODEL_TYPE is last, as it names the client rather than the host, which
		// differ when one client serves several hosts
		if (servingProvider == null && smssProp != null) {
			servingProvider = StaticBuiltinToolsCatalog
					.normalizeProviderKey(ValueUtils.trimToNull(smssProp.getProperty(IModelEngine.MODEL_TYPE)));
		}
		return servingProvider;
	}

	/**
	 * Reads the properties of an engine's smss file, found by the path the engine
	 * was loaded with.
	 *
	 * @param engineId the engine
	 * @return its smss properties, or null when they cannot be read
	 */
	public static Properties loadSmssProperties(String engineId) {
		if (engineId == null) {
			return null;
		}
		Object smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		if (smssFile == null) {
			return null;
		}
		try {
			return Utility.loadProperties(smssFile.toString());
		} catch (Exception e) {
			classLogger.warn("Unable to read the smss file for engine {}", Utility.cleanLogString(engineId), e);
			return null;
		}
	}

	private static String fromMetadata(Map<String, Object> metadata) {
		return metadata == null ? null
				: StaticBuiltinToolsCatalog
						.normalizeProviderKey(ValueUtils.trimToNull(metadata.get("servingProvider")));
	}
}
