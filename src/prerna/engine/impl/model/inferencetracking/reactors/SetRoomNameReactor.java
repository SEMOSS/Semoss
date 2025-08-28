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
package prerna.engine.impl.model.inferencetracking.reactors;

import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetRoomNameReactor extends AbstractReactor {

	public SetRoomNameReactor() {
		this.keysToGet = new String[]{"roomId", "roomName"};
		this.keyRequired = new int[]{1, 1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String roomId = this.keyValue.get(this.keysToGet[0]);
		String roomName = this.keyValue.get(this.keysToGet[1]);
		boolean output = ModelInferenceLogsUtils.doSetNameForRoom(user.getPrimaryLoginToken().getId(), roomId,
				roomName);
		return new NounMetadata(output, PixelDataType.BOOLEAN);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("roomId")) {
			return "The room or conversation ID for a given chat app";
		} else if (key.equals("roomName")) {
			return "A sequence of characters to set as the new name";
		}
		return super.getDescriptionForKey(key);
	}
}
