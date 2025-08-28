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
package prerna.reactor.map;

import java.util.Hashtable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MapMapReactor extends AbstractMapReactor {

	// this could come in here as a simple vector
	// or it could come in here as a hashtable
	// need to see which one to instantiate

	// I also need to see the parent reactor
	// what it is and appropriately add the stuff

	@Override
	public NounMetadata execute() {

		// couple of different cases here
		// if Hashtable property is set to true
		// then this needs to be processed as hash
		// once you get the hash
		// you need to add it to the parent

		// you could add it with the curnoun I bet
		NounMetadata noun = null;

		Hashtable output = this.store.getDataHash();
		output.remove("all");
		noun = new NounMetadata(output, PixelDataType.MAP);

		return noun;
	}
}
