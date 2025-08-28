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

import java.io.File;

public class DeleteInsightImages {

	public static void main(String[] args) {
		// USER INPUT!
		// Set the db folder location
		String dbPath = "C:\\workspace\\Semoss_Dev\\db";

		File allDbFolder = new File(dbPath);
		if (!allDbFolder.exists()) {
			System.out.println("YOU PROBABLY FORGOT TO UPDATE THE DB PATH VARIABLE!!!");
			System.out.println("YOU PROBABLY FORGOT TO UPDATE THE DB PATH VARIABLE!!!");
			System.out.println("YOU PROBABLY FORGOT TO UPDATE THE DB PATH VARIABLE!!!");
			System.out.println("YOU PROBABLY FORGOT TO UPDATE THE DB PATH VARIABLE!!!");
			System.out.println("YOU PROBABLY FORGOT TO UPDATE THE DB PATH VARIABLE!!!");
			return;
		}

		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

		// grab all the files in the db folder
		File[] allFiles = allDbFolder.listFiles();
		for (File dbFolder : allFiles) {
			// grab all the folders
			if (dbFolder.isDirectory()) {
				// see if a version folder exists
				String dbVersionPath = dbFolder.getAbsolutePath() + DIR_SEPARATOR + "version";
				File dbVersion = new File(dbVersionPath);
				// see version folder is there
				if (dbVersion.exists()) {
					// grab all the files in the version folder
					File[] allVersionFiles = dbVersion.listFiles();
					for (File insightFolder : allVersionFiles) {
						// grab all the folders
						if (insightFolder.isDirectory()) {
							String insightImagePath = insightFolder.getAbsolutePath() + DIR_SEPARATOR + "image.png";
							File insightImage = new File(insightImagePath);
							if (insightImage.exists()) {
								// deleting an insight
								System.out.println("Deleting ... " + insightImagePath);
								insightImage.delete();
							}
						}
					}
				}
			}
		}
		System.out.println("Done deleting images");
	}
}
