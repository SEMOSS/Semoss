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
package prerna.util.usertracking.reactors;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

/**
 * Generates column descriptions and stores in the tracking database Adds unique
 * count to owl file for each column
 *
 */
public class ExtractDatabaseMetaReactor extends AbstractRFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(ExtractDatabaseMetaReactor.class);

	private static final String CLASS_NAME = ExtractDatabaseMetaReactor.class.getName();

	public ExtractDatabaseMetaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get inputs - engine
		String engineId = UploadInputUtility.getEngineNameOrId(this.store,
				this.keyValue.get(ReactorKeysEnum.DATABASE.getKey()));
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Database " + engineId + " does not exist or user does not have access to database");
		}

		IDatabaseEngine engine = Utility.getDatabase(engineId);
		// validate engine exists
		if (engine == null) {
			throw new IllegalArgumentException("Engine does not exist");
		}

		// only executes for rdbms, tinker, and rdf
		DATABASE_TYPE engineType = engine.getDatabaseType();
		if (engineType == DATABASE_TYPE.RDBMS || engineType == DATABASE_TYPE.SESAME
				|| engineType == DATABASE_TYPE.TINKER) {
			try (WriteOWLEngine owlEngine = engine.getOWLEngineFactory().getWriteOWL()) {
				owlEngine.addUniqueCounts(engine);
			} catch (IOException | InterruptedException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getName() {
		return "ExtractDatabaseMeta";
	}

}
