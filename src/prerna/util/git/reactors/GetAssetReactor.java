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
package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.FileUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class GetAssetReactor extends AbstractReactor {

	// gets a particular asset in a particular version
	// if the version is not provided - this gets the head

	public GetAssetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.VERSION.getKey(),
				ReactorKeysEnum.SPACE.getKey()};
		this.keyRequired = new int[]{1, 0, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// grab the version
		String version = null;
		if (this.keyValue.containsKey(ReactorKeysEnum.VERSION.getKey())) {
			version = this.keyValue.get(ReactorKeysEnum.VERSION.getKey());
		}

		// specify a file
		String asset = Utility.normalizePath(this.keyValue.get(keysToGet[0]));
		if (!asset.startsWith("/") && !asset.startsWith("\\")) {
			asset = "/" + asset;
		}

		// check if user is logged in
		String space = this.keyValue.get(this.keysToGet[2]);
		// we need to change this to asset base folder
		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false);

		String output = null;
		if (version != null) {
			// I need a better way than output
			// probably write the file and volley the file ?
			// ideally this should be through the sym link
			output = GitRepoUtils.getFile(version, asset, assetFolder);
		} else {
			// just read the current file
			String assetFilePath = assetFolder + asset;
			try {
				output = FileUtils.readFileToString(new File(assetFilePath), Charset.forName("UTF-8"));
			} catch (IOException e) {
				throw new IllegalArgumentException("Unable to read file " + asset);
			}
		}

		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
