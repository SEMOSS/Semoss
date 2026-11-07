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
package prerna.reactor.model.serving;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.serving.ServedModelGuard;
import prerna.reactor.model.serving.ServedModelEngineLinks.ModelEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The model engines registered with this SEMOSS, read from their smss files
 * without loading them (loading a model engine starts its client), and the link
 * to a served model written back to them.
 */
final class ServedModelEngineStore {

	private static final Logger classLogger = LogManager.getLogger(ServedModelEngineStore.class);

	private ServedModelEngineStore() {
	}

	/** Every model engine whose smss file can be read; the others are logged and left out. */
	static List<ModelEngine> modelEngines() {
		List<ModelEngine> engines = new ArrayList<>();
		for (String engineId : SecurityEngineUtils.getAllEngineIds(List.of(IEngine.CATALOG_TYPE.MODEL.name()))) {
			Object smss = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
			if (!(smss instanceof String path) || path.isBlank() || !Files.isRegularFile(Path.of(path))) {
				continue;
			}
			try {
				engines.add(ServedModelEngineLinks.read(engineId, Path.of(path)));
			} catch (IOException | IllegalArgumentException e) {
				classLogger.warn("Cannot read the smss of model engine {}: {}", engineId, e.getMessage());
			}
		}
		return engines;
	}

	/** The model engine with this id; refused when there is none. */
	static ModelEngine modelEngine(String engineId) {
		return modelEngines().stream().filter(engine -> engine.engineId().equals(engineId)).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("There is no model engine with the id " + engineId));
	}

	/**
	 * Link a model engine to a served model, or unlink it when the served model is
	 * null: in its smss file, in the engine itself when it is loaded (its ask guard
	 * reads the properties from memory, so it needs no restart), and in the
	 * cluster's copy of the smss.
	 *
	 * @return false when the engine already was, and nothing changed
	 */
	static synchronized boolean link(ModelEngine engine, String namespace, String servedModel) {
		try {
			if (!ServedModelEngineLinks.writeLink(engine.smss(), namespace, servedModel)) {
				return false;
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"The smss file of " + engine.engineName() + " could not be changed: " + e.getMessage(), e);
		}
		if (DIHelper.getInstance().getEngineProperty(engine.engineId()) instanceof IEngine loaded) {
			setLink(loaded.getSmssProp(), namespace, servedModel);
			setLink(loaded.getOrigSmssProp(), namespace, servedModel);
		}
		ClusterUtil.pushEngineSmss(engine.engineId());
		return true;
	}

	private static void setLink(Properties properties, String namespace, String servedModel) {
		if (properties == null) {
			return;
		}
		if (servedModel == null) {
			properties.remove(ServedModelGuard.SERVED_MODEL_PROPERTY);
			properties.remove(ServedModelGuard.NAMESPACE_PROPERTY);
		} else {
			properties.setProperty(ServedModelGuard.NAMESPACE_PROPERTY, namespace);
			properties.setProperty(ServedModelGuard.SERVED_MODEL_PROPERTY, servedModel);
		}
	}
}
