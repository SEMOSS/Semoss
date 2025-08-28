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
package prerna.util;

import java.util.Vector;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import prerna.algorithm.api.SemossDataType;
import prerna.reactor.AssignmentReactor;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyGraphIoRegistry extends AbstractIoRegistry {

	public MyGraphIoRegistry() {
		register(GryoIo.class, Vector.class, null);
		register(GryoIo.class, SemossDataType.class, null);
		register(GryoIo.class, IReactor.TYPE.class, null);
		register(GryoIo.class, AssignmentReactor.class, null);
		register(GryoIo.class, GenRowStruct.class, null);
		register(GryoIo.class, NounMetadata.class, null);
		register(GryoIo.class, PixelDataType.class, null);
	}
}
