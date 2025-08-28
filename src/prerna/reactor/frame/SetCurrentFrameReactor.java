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
package prerna.reactor.frame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetCurrentFrameReactor extends AbstractFrameReactor {

	public SetCurrentFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame dm = getFrame();
		if (dm == null) {
			throw new IllegalArgumentException("Could not find frame to set");
		}
		// set the new main dm for the insight
		this.insight.setDataMaker(dm);
		NounMetadata noun = new NounMetadata(dm, PixelDataType.FRAME, PixelOperationType.FRAME);
		noun.addAdditionalReturn(
				new NounMetadata("New frame has been set", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
