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
package prerna.engine.impl.rdbms;

import prerna.engine.api.IDatabaseEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.ImpalaSqlInterpreter;

@Deprecated
public class ImpalaEngine extends RDBMSNativeEngine {

	/*
	 * Reviewed 2023-07-18 We do not need this class anymore, we have moved this to
	 * the query util class in RDBMSNativeEngine
	 */

	/*
	 * WE ONLY HAVE THIS CLASS BECAUSE OF THE QUEYR WRAPPER WEIRD WORD LOWER CASEING
	 * HAPPENS FROM THE RESUTL SET METADATA SO WE HAVE OUR OWN ENGINE AND IT HAS ITS
	 * OWN ENGINE TYPE WITH ASSOCIATED WRAPPER
	 *
	 * PLEASE TRY TO USE THE DEFAULT RDBMSNativeEngine WHEN POSSIBLE INSTEAD OF
	 * MAKING A NEW CLASS
	 *
	 */

	@Deprecated
	public ImpalaEngine() {
	}

	@Deprecated
	public IQueryInterpreter getQueryInterpreter() {
		return new ImpalaSqlInterpreter(this);
	}

	@Deprecated
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.IMPALA;
	}
}
