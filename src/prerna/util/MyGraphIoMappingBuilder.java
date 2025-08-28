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

import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;

public class MyGraphIoMappingBuilder implements Consumer<Mapper.Builder> {

	@Override
	public void accept(Mapper.Builder t) {
		t.addRegistry(new MyGraphIoRegistry());
	}
}
