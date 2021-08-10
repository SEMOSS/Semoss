package prerna.poi.main.helper.excel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.crypt.EncryptionMode;
import org.apache.poi.poifs.crypt.Encryptor;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Utility;

public class ExcelUtility {

	public static void encrypt(Workbook workbook, String fileLocation, String password) {
		FileOutputStream output = null;
		POIFSFileSystem fs = null;
		OPCPackage opc = null;
		ByteArrayInputStream input = null;
		OutputStream os  = null;
		try {
			// saves sheet
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			workbook.write(bos);
			workbook.close();
			input = new ByteArrayInputStream(bos.toByteArray());
			bos.close();
			output = new FileOutputStream(fileLocation);
			// encrypt the file
			fs = new POIFSFileSystem();
			EncryptionInfo info = new EncryptionInfo(EncryptionMode.agile);
			Encryptor enc = info.getEncryptor();
			enc.confirmPassword(password);
			opc = OPCPackage.open(input);
			os = enc.getDataStream(fs);
			opc.save(os);
			fs.writeFilesystem(output);
		} catch (GeneralSecurityException e) {
			throw new RuntimeException(e);
		} catch (InvalidFormatException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(output != null) {
		          try {
		        	  output.close();
		          } catch(IOException e) {
		        	  e.printStackTrace();
		          }
		        }
			if(fs != null) {
		          try {
		        	  fs.close();
		          } catch(IOException e) {
		        	  e.printStackTrace();
		          }
		        }
			if(opc != null) {
		          try {
		        	  opc.close();
		          } catch(IOException e) {
		        	  e.printStackTrace();
		          }
		        }
			if(input != null) {
		          try {
		        	  input.close();
		          } catch(IOException e) {
		        	  e.printStackTrace();
		          }
		        }
			if(os != null) {
		          try {
		        	  os.close();
		          } catch(IOException e) {
		        	  e.printStackTrace();
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
			e.printStackTrace();
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (workbook != null) {
				try {
					workbook.close();
					workbook.dispose();
				} catch (IOException e) {
					e.printStackTrace();
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (workbook != null) {
				try {
					workbook.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

}
