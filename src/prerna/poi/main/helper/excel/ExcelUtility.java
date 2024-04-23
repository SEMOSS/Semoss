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

import prerna.util.Constants;
import prerna.util.Utility;

public class ExcelUtility {

	private static final Logger classLogger = LogManager.getLogger(ExcelUtility.class);

	/**
	 * 
	 * @param fileLocation
	 * @return
	 */
	public static boolean isExcelEncrypted(String fileLocation) {
		boolean isEncrypted = false;
		try (POIFSFileSystem x = new POIFSFileSystem(new FileInputStream(fileLocation))  ) { 
			isEncrypted = true;
		} catch(OfficeXmlFileException e) {
			// This is a regular ooxml .xlsx file
			isEncrypted = false;
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		
		return isEncrypted;
	}
	
	/**
	 * 
	 * @param workbook
	 * @param fileLocation
	 * @param password
	 */
	public static void encrypt(Workbook workbook, String fileLocation, String password) {
		POIFSFileSystem fs = null;
		OutputStream os  = null;
		OutputStream encos  = null;
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
			
			os = new FileOutputStream(Utility.normalizePath(fileLocation));
			fs.writeFilesystem(os);
		} catch (GeneralSecurityException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(workbook != null) {
				try {
					workbook.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(os != null) {
				try {
					os.close();
				} catch(IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(fs != null) {
				try {
					fs.close();
				} catch(IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Write the file
	 * Also closes the workbook so no additional changes can be performed
	 * @param workbook
	 * @param fileLocation
	 */
	public static void writeToFile(SXSSFWorkbook workbook, String fileLocation) {
		fileLocation = Utility.normalizePath(fileLocation);
		// make sure the directory exists
		{
			File file = new File(fileLocation);
			if(!file.getParentFile().exists() || !file.getParentFile().isDirectory()) {
				file.getParentFile().mkdirs();
			}
		}

		FileOutputStream fileOut = null;
		try {
			fileOut = new FileOutputStream(fileLocation);
			workbook.write(fileOut);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (workbook != null) {
				try {
					workbook.close();
					workbook.dispose();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Write the file
	 * Also closes the workbook so no additional changes can be performed
	 * @param workbook
	 * @param fileLocation
	 */
	public static void writeToFile(XSSFWorkbook workbook, String fileLocation) {
		fileLocation = Utility.normalizePath(fileLocation);
		// make sure the directory exists
		{
			File file = new File(fileLocation);
			if(!file.getParentFile().exists() || !file.getParentFile().isDirectory()) {
				file.getParentFile().mkdirs();
			}
		}

		FileOutputStream out = null;
		try {
			out = new FileOutputStream(fileLocation);
			workbook.write(out);
		} catch (FileNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if (workbook != null) {
				try {
					workbook.close();
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

}
