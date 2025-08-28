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
package prerna.query.interpreters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.query.querystruct.AbstractQueryStruct;

public abstract class AbstractQueryInterpreter implements IQueryInterpreter {

	protected Logger logger = null;

	protected int performCount;
	protected AbstractQueryStruct qs;
	protected boolean isDistinct;

	public AbstractQueryInterpreter() {
		logger = LogManager.getLogger(this.getClass().getName());
	}

	@Override
	public void setQueryStruct(AbstractQueryStruct qs) {
		this.qs = qs;
	}

	@Override
	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}

	@Override
	public boolean isDistinct() {
		return this.isDistinct;
	}

	@Override
	public void setLogger(Logger logger) {
		if (logger != null) {
			this.logger = logger;
		}
	}
}
