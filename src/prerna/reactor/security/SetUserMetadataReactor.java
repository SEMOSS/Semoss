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
package prerna.reactor.security;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import prerna.auth.AuthProvider;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetUserMetadataReactor extends AbstractSetMetadataReactor {

	public SetUserMetadataReactor() {
		this.keysToGet = new String[]{"userId", ReactorKeysEnum.PROVIDER.getKey(), META,
				ReactorKeysEnum.ENCODED.getKey(), ReactorKeysEnum.JSON_CLEANUP.getKey()};
		this.keyRequired = new int[]{1, 1, 1, 0, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String userId = StringUtils.trimToNull(this.keyValue.get("userId"));
		String userType = StringUtils.trimToNull(this.keyValue.get(ReactorKeysEnum.PROVIDER.getKey()));

		if (userId == null || userType == null) {
			throw new IllegalArgumentException("userId and provider required");
		}

		if (!SecurityAdminUtils.userIsAdmin(this.insight.getUser())) {
			throw new IllegalArgumentException("User must be an admin to edit");
		}

		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityUserUtils.getAllMetakeys();
		if (!validMetakeys.containsAll(metadata.keySet())) {
			throw new IllegalArgumentException("Unallowed metakeys. Can only use: " + String.join(", ", validMetakeys));
		}

		SecurityUserUtils.updateUserMetadata(userId, AuthProvider.getProviderFromString(userType), metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the user"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Define metadata on a user";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the user. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}
}
