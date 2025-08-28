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
import prerna.auth.utils.SecurityProjectUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class SetProjectMetadataReactor extends AbstractSetMetadataReactor {

	public SetProjectMetadataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), META, ReactorKeysEnum.ENCODED.getKey(),
				ReactorKeysEnum.JSON_CLEANUP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = UploadInputUtility.getProjectNameOrId(this.store);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to edit");
		}

		Map<String, Object> metadata = getMetaMap();
		// check for invalid metakeys
		List<String> validMetakeys = SecurityProjectUtils.getAllMetakeys();
		if (!validMetakeys.containsAll(metadata.keySet())) {
			throw new IllegalArgumentException("Unallowed metakeys. Can only use: " + String.join(", ", validMetakeys));
		}

		SecurityProjectUtils.updateProjectMetadata(projectId, metadata);
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(
				NounMetadata.getSuccessNounMessage("Successfully set the new metadata values for the project"));
		return noun;
	}

	@Override
	public String getReactorDescription() {
		return "Define metadata on a project";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(META)) {
			return "Map containing {'metaKey':['value1','value2', etc.]} containing the list of metadata values to define on the database. The list of values will determine the order that is defined for field";
		}
		return super.getDescriptionForKey(key);
	}
}
