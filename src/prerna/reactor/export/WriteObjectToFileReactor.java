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
package prerna.reactor.export;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class WriteObjectToFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(WriteObjectToFileReactor.class);

	public WriteObjectToFileReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.ENCODED.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		Object value = getObject();
		if (isEncoded()) {
			value = Utility.decodeURIComponent(value + "");
		}
		File f = new File(filePath);
		try {
			FileUtils.writeStringToFile(f, value.toString(), "UTF-8");
			return new NounMetadata(f.getName(), PixelDataType.CONST_STRING);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to write object to file");
		}
	}

	/**
	 * @return
	 */
	private Object getObject() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VALUE.getKey());
		if (grs != null && !grs.isEmpty()) {
			return grs.get(0);
		}

		if (!this.curRow.isEmpty()) {
			return this.curRow.get(0);
		}

		throw new NullPointerException("Must define the object to write to file");
	}

	/**
	 * @return
	 */
	private boolean isEncoded() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ENCODED.getKey());
		if (grs != null && !grs.isEmpty()) {
			return (Boolean) grs.get(0);
		}

		return false;
	}
}
