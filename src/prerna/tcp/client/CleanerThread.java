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
package prerna.tcp.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;
import prerna.util.Constants;

public class CleanerThread extends Thread {

	// store the folder to delete
	public String folder = null;

	private static final Logger classLogger = LogManager.getLogger(CleanerThread.class);

	public CleanerThread(String folder) {
		this.folder = folder;
	}

	@Override
	public void run() {
		int attempt = 1;
		boolean deleted = false;
		while (attempt < 10 && !deleted) {
			try {
				FileUtils.deleteDirectory(folder);
				deleted = true;
			} catch (Exception ignored) {
				attempt++;
				try {
					Thread.sleep(attempt * 1000);
				} catch (InterruptedException e1) {
					classLogger.error(Constants.STACKTRACE, e1);
				}
			}
		}

		if (attempt >= 10) {
			classLogger.error(Constants.STACKTRACE, "Unable to delete directory on netty cleanup: " + folder);
		} else {
			classLogger.info("Deleted directory " + folder);
		}
	}
}
