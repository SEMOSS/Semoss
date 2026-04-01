/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.shortcuts.fileupload.job;

import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;

public class FileWatcherReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(FileWatcherReactor.class);

	public FileWatcherReactor() {

		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		String filePath = this.keyValue.get(UploadInputUtility.FILE_PATH);

		try {

			FileWatchServiceFactory factory = FileWatchServiceFactory.getInstance();

			factory.start();

			FileWatcherManager manager = factory.getManager();

			manager.addDirectory(Path.of(filePath));
			manager.addDirectory(Path.of(filePath));

			Thread.sleep(5000);
			manager.pauseDirectory(Path.of(filePath));

			Thread.sleep(5000);
			manager.resumeDirectory(Path.of(filePath));

			Thread.sleep(5000);
			manager.removeDirectory(Path.of(filePath));

			Thread.sleep(5000);
			factory.restart();

			Thread.sleep(5000);
			factory.shutdown();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

}
