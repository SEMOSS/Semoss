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
package prerna.engine.impl.vector;

import java.io.File;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.frame.gaas.processors.AbstractFileProcessor;
import prerna.reactor.frame.gaas.processors.IFileProcessor;
import prerna.util.Constants;

public class VectorDatabaseUtils {

	private static final Logger classLogger = LogManager.getLogger(VectorDatabaseUtils.class);

	/**
	 * @param csvFileName
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int convertFilesToCSV(String csvFileName, File file) throws Exception {
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(csvFileName);
		try {
			classLogger.info("Processing file : " + file.getName());
			IFileProcessor processor = AbstractFileProcessor.getFileProcessor(file, writer);
			if (processor != null) {
				processor.process();
				classLogger.info("Completed Processing file : " + file.getAbsolutePath());
			} else {
				classLogger.info("No file processor for file : " + file.getAbsolutePath());
			}
		} catch (NullPointerException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			writer.close();
		}

		return writer.getRowsInCsv();
	}
}
