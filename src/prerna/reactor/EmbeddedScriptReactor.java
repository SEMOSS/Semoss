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
package prerna.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EmbeddedScriptReactor extends AbstractReactor {

	// this class does nothing!
	// it is meant when we have an embedded script
	// within our main script
	// but i just want to push the output of this to the main script

	@Override
	public NounMetadata execute() {
		int size = this.curRow.size();
		NounMetadata n = this.curRow.getNoun(size - 1);
		if (n.getNounType() == PixelDataType.LAMBDA) {
			n = ((IReactor) n.getValue()).execute();
		}
		return n;
	}

	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		if (parentReactor != null) {
			int size = this.curRow.size();
			NounMetadata n = this.curRow.getNoun(size - 1);
			this.parentReactor.getCurRow().add(n);
		}
	}
}
