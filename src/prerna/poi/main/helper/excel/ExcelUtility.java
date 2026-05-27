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
package prerna.poi.main.helper.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Utility;

/**
 * Utility methods for common Excel file operations such as encryption and file
 * writes.
 */
public class ExcelUtility {

	private static final Logger classLogger = LogManager.getLogger(ExcelUtility.class);

	/**
	 * Checks whether the supplied workbook file is encrypted.
	 *
	 * @param fileLocation absolute or relative workbook path
	 * @return {@code true} when the workbook is encrypted, {@code false} when it is
	 *         a regular OOXML workbook
	 */
	public static boolean isExcelEncrypted(String fileLocation) {
		String normalizedPath = Utility.normalizePath(fileLocation);
		try (POIFSFileSystem x = new POIFSFileSystem(new FileInputStream(normalizedPath))) {
			return true;
		} catch (OfficeXmlFileException e) {
			// This is a regular ooxml .xlsx file
			return false;
		} catch (IOException e) {
			classLogger.error("Failed to determine whether Excel file is encrypted: {}", normalizedPath, e);
			throw new IllegalArgumentException("Could not handle file location. See logs for details.", e);
		}
	}

	/**
	 * Encrypts and writes a workbook to disk using the provided password.
	 *
	 * @param workbook     workbook to encrypt and write
	 * @param fileLocation target workbook file path
	 * @param password     password used for workbook encryption
	 */
	public static void encrypt(Workbook workbook, String fileLocation, String password) {
		String normalizedPath = Utility.normalizePath(fileLocation);
		POIFSFileSystem fs = null;
		OutputStream os = null;
		OutputStream encos = null;
		try {
			fs = new POIFSFileSystem();
			EncryptionInfo info = new EncryptionInfo(EncryptionMode.agile);
			Encryptor enc = info.getEncryptor();
			enc.confirmPassword(password);

			// write the workbook into an encrypted outputstream
			encos = enc.getDataStream(fs);
			workbook.write(encos);
			workbook.close();
			encos.close();

			os = new FileOutputStream(normalizedPath);
			fs.writeFilesystem(os);
		} catch (GeneralSecurityException e) {
			classLogger.error("Failed to encrypt workbook for file: {}", normalizedPath, e);
			throw new RuntimeException("Failed to encrypt workbook for file: " + normalizedPath, e);
		} catch (IOException e) {
			classLogger.error("Failed to write encrypted workbook to file: {}", normalizedPath, e);
		} finally {
			if (workbook != null) {
				try {
					workbook.close();
				} catch (IOException e) {
					classLogger.error("Failed to close workbook after encryption for file: {}", normalizedPath, e);
				}
			}
			if (os != null) {
				try {
					os.close();
				} catch (IOException e) {
					classLogger.error("Failed to close output stream for encrypted workbook: {}", normalizedPath, e);
				}
			}
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					classLogger.error("Failed to close POIFS filesystem for encrypted workbook: {}", normalizedPath, e);
				}
			}
		}
	}

	/**
	 * Write the file Also closes the workbook so no additional changes can be
	 * performed
	 * 
	 * @param workbook     streaming workbook instance
	 * @param fileLocation target workbook file path
	 */
	public static void writeToFile(SXSSFWorkbook workbook, String fileLocation) {
		fileLocation = Utility.normalizePath(fileLocation);
		// make sure the directory exists
		{
			File file = new File(fileLocation);
			if (!file.getParentFile().exists() || !file.getParentFile().isDirectory()) {
				file.getParentFile().mkdirs();
			}
		}

		FileOutputStream fileOut = null;
		try {
			fileOut = new FileOutputStream(fileLocation);
			workbook.write(fileOut);
		} catch (IOException e) {
			classLogger.error("Failed to write SXSSFWorkbook to file: {}", fileLocation, e);
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
					classLogger.error("Failed to close file output stream for SXSSFWorkbook file: {}", fileLocation, e);
				}
			}
			if (workbook != null) {
				try {
					workbook.close();
					workbook.dispose();
				} catch (IOException e) {
					classLogger.error("Failed to close SXSSFWorkbook after writing file: {}", fileLocation, e);
				}
			}
		}
	}

	/**
	 * Write the file Also closes the workbook so no additional changes can be
	 * performed
	 * 
	 * @param workbook     workbook instance
	 * @param fileLocation target workbook file path
	 */
	public static void writeToFile(XSSFWorkbook workbook, String fileLocation) {
		fileLocation = Utility.normalizePath(fileLocation);
		// make sure the directory exists
		{
			File file = new File(fileLocation);
			if (!file.getParentFile().exists() || !file.getParentFile().isDirectory()) {
				file.getParentFile().mkdirs();
			}
		}

		FileOutputStream out = null;
		try {
			out = new FileOutputStream(fileLocation);
			workbook.write(out);
		} catch (FileNotFoundException e) {
			classLogger.error("Workbook output path not found: {}", fileLocation, e);
		} catch (IOException e) {
			classLogger.error("Failed to write XSSFWorkbook to file: {}", fileLocation, e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					classLogger.error("Failed to close file output stream for XSSFWorkbook file: {}", fileLocation, e);
				}
			}
			if (workbook != null) {
				try {
					workbook.close();
				} catch (Exception e) {
					classLogger.error("Failed to close XSSFWorkbook after writing file: {}", fileLocation, e);
				}
			}
		}
	}

}
