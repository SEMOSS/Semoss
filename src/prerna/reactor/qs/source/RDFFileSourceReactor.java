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
package prerna.reactor.qs.source;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.TemporalEngineHardQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Constants;
import prerna.util.Utility;

public class RDFFileSourceReactor extends AbstractQueryStructReactor {

	private static final Logger classLogger = LogManager.getLogger(RDFFileSourceReactor.class);

	public static final String RDF_TYPE = "rdfType";
	public static final String BASE_URI = "baseUri";

	public RDFFileSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), RDF_TYPE, BASE_URI,
				ReactorKeysEnum.QUERY_KEY.getKey() };
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();

		// need to maintain what the FE passed to create this
		String filePath = Utility.normalizePath(this.keyValue.get(this.keysToGet[0]));
		String rdfFileType = this.keyValue.get(this.keysToGet[1]);
		if (rdfFileType == null || rdfFileType.isEmpty()) {
			rdfFileType = "RDF/XML";
		}
		String baseURI = this.keyValue.get(this.keysToGet[2]);
		String query = this.keyValue.get(this.keysToGet[3]);

		Map<String, Object> config = new HashMap<String, Object>();
		config.put(this.keysToGet[0], filePath);
		config.put(this.keysToGet[1], rdfFileType);
		config.put(this.keysToGet[2], baseURI);

		filePath = this.insight.getAbsoluteInsightFolderPath(filePath);
		File file = new File(filePath);
		if (!file.exists()) {
			throw new IllegalArgumentException("Unable to location file");
		}

		// generate the in memory rc
		RepositoryConnection rc = null;
		try {
			Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			rc = myRepository.getConnection();

			// load in the meta from saved file
			if (rdfFileType.equalsIgnoreCase("RDF/XML")) {
				rc.add(file, baseURI, RDFFormat.RDFXML);
			} else if (rdfFileType.equalsIgnoreCase("TURTLE")) {
				rc.add(file, baseURI, RDFFormat.TURTLE);
			} else if (rdfFileType.equalsIgnoreCase("BINARY")) {
				rc.add(file, baseURI, RDFFormat.BINARY);
			} else if (rdfFileType.equalsIgnoreCase("N3")) {
				rc.add(file, baseURI, RDFFormat.N3);
			} else if (rdfFileType.equalsIgnoreCase("NTRIPLES")) {
				rc.add(file, baseURI, RDFFormat.NTRIPLES);
			} else if (rdfFileType.equalsIgnoreCase("TRIG")) {
				rc.add(file, baseURI, RDFFormat.TRIG);
			} else if (rdfFileType.equalsIgnoreCase("TRIX")) {
				rc.add(file, baseURI, RDFFormat.TRIX);
			}
		} catch (RuntimeException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (RepositoryException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (RDFParseException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// set the rc in the in-memory engine
		InMemorySesameEngine temportalEngine = new InMemorySesameEngine();
		temportalEngine.setRepositoryConnection(rc);
		temportalEngine.setEngineId("FAKE_ENGINE");
		temportalEngine.setBasic(true);

		TemporalEngineHardQueryStruct qs = new TemporalEngineHardQueryStruct();
		qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_RDF_FILE_ENGINE_QUERY);
		qs.setConfig(config);
		qs.setEngine(temportalEngine);
		if (query != null && !query.isEmpty()) {
			qs.setQuery(query);
		}
		this.qs = qs;
		return qs;
	}

}
