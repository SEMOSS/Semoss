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
package prerna.sablecc2.om.task;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import prerna.engine.api.IHeadersDataRow;

public abstract class AbstractTaskOperation extends AbstractTask {

	protected transient ITask innerTask;

	public AbstractTaskOperation() {
	}

	public AbstractTaskOperation(ITask innerTask) {
		setInnerTask(innerTask);
	}

	/**
	 * Get all the props from the original task
	 *
	 * @param innerTask
	 */
	private void consumeInnerTask(ITask innerTask) {
		this.formatter = innerTask.getFormatter();
		this.taskOptions = innerTask.getTaskOptions();
		this.headerInfo = innerTask.getHeaderInfo();
		this.sortInfo = innerTask.getSortInfo();
		this.filterInfo = innerTask.getFilterInfo();
	}

	@Override
	public boolean hasNext() {
		// base implementation
		return this.innerTask.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		// base implementation
		this.internalOffset++;
		return this.innerTask.next();
	}

	@Override
	public void close() throws IOException {
		this.innerTask.close();
	}

	@Override
	public void reset() throws Exception {
		this.innerTask.reset();
	}

	public void setInnerTask(ITask innerTask) {
		this.innerTask = innerTask;
		consumeInnerTask(innerTask);
	}

	public ITask getInnerTask() {
		return this.innerTask;
	}

	@Override
	public List<Map<String, String>> getSource() {
		// TODO Auto-generated method stub
		return null;
	}
}
