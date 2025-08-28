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
package prerna.engine.api;

import prerna.reactor.IReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public interface IReactorFunctionEngine extends IReactor, IFunctionEngine {

	// things to think about
	// how do use this as a reactor - given the lifecycle portion of it
	// may be one of the pieces is the id to get to it
	// it is almost I have to go from reactor -- engine -- reactor (this is really
	// just the engine)
	// may be that is the answer the first reactor is just a wrapper which
	// ultimately just passes
	// everything to this guy
	// by setting the nounstore
	// the initial one is purely just a pass through

	/**
	 * Execute with the provided noun store
	 *
	 * @param ns
	 * @return
	 */
	NounMetadata execute(NounStore ns);

	/**
	 * Execute with the provided noun store and curRow
	 *
	 * @param ns
	 * @param curRow
	 * @return
	 */
	NounMetadata execute(NounStore ns, GenRowStruct curRow);
}
