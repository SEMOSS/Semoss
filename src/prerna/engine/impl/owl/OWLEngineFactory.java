/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.owl;

import java.util.concurrent.Semaphore;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;

public class OWLEngineFactory {

	private final Semaphore writeSemaphore = new Semaphore(1);

	private RDFFileSesameEngine baseDataEngine = null;
	private IDatabaseEngine.DATABASE_TYPE dbType = IDatabaseEngine.DATABASE_TYPE.RDBMS;
	private String engineId = null;
	private String engineName = null;

	private ReadOnlyOWLEngine reader = null;
	private WriteOWLEngine writer = null;

	public OWLEngineFactory(RDFFileSesameEngine baseDataEngine, IDatabaseEngine.DATABASE_TYPE dbType, String engineId,
			String engineName) {
		this.baseDataEngine = baseDataEngine;
		this.dbType = dbType;
		this.engineId = engineId;
		this.engineName = engineName;

		this.reader = new ReadOnlyOWLEngine(this.baseDataEngine, this.engineId, this.engineName);
		this.writer = new WriteOWLEngine(this.writeSemaphore, this.baseDataEngine, this.dbType, this.engineId,
				this.engineName);
	}

	/**
	 * @return
	 */
	public ReadOnlyOWLEngine getReadOWL() {
		return this.reader;
	}

	/**
	 * Provides a construct to allow writes to the OWL Must close the WriteOWLEngine
	 * to release the lock
	 *
	 * @return
	 * @throws InterruptedException
	 */
	public WriteOWLEngine getWriteOWL() throws InterruptedException {
		this.writeSemaphore.acquire();
		return this.writer;
	}
}
