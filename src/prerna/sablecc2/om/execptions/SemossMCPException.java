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
package prerna.sablecc2.om.execptions;

import prerna.reactor.agent.mcp.MCPErrorCode;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SemossMCPException extends SemossPixelException {

	private MCPErrorCode error = null;

	public SemossMCPException(String message, MCPErrorCode error) {
		super(message);
		this.error = error;
	}

	public SemossMCPException(String message, Throwable e, MCPErrorCode error) {
		super(message, e);
		this.error = error;
	}

	public SemossMCPException(Throwable cause, MCPErrorCode error) {
		super(cause);
		this.error = error;
	}

	public SemossMCPException(String message, boolean continueThreadOfExecution, MCPErrorCode error) {
		super(message);
		this.error = error;
	}

	public SemossMCPException(NounMetadata noun, MCPErrorCode error) {
		super(noun);
		this.error = error;
	}

	/**
	 * @return
	 */
	public MCPErrorCode getError() {
		return this.error;
	}

	/** Always kill the thread of execution */
	@Override
	public boolean isContinueThreadOfExecution() {
		return false;
	}
}
