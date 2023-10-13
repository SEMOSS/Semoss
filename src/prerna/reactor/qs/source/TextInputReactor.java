package prerna.reactor.qs.source;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class TextInputReactor extends AbstractQueryStructReactor {

	//keys to get inputs from pixel command
	private static final String FILE_INFO = "fileData";
	private static final String DATA_TYPES = "dataTypeMap";
	private static final String DELIMITER = "delim";

	/**
	 * TextInput args 
	 * 
	 * FILE_INFO=["fileInfo"]
	 * DELIMITER = ["delimiter"]
	 * 
	 * to set dataTypes 
	 *     dataTypesMap = [{"column", "type"}]
	 */

	@Override
	protected SelectQueryStruct createQueryStruct() {
		CsvQueryStruct qs = null;

		// get inputs
		Map<String, String> dataTypes = getDataTypes(); 
		String fileInfo = getFileInfo();

		// write the file on disk
		Date date = new Date();
		String modifiedDate = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").format(date);
		String fileLocation = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "PastedData" + modifiedDate + ".csv";
		File file = new File(fileLocation);
		FileWriter fw = null;
		try {
			fw = new FileWriter(file);
			fw.write(fileInfo);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fw != null) {
				try {
					fw.flush();
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// set csv qs
		char delimiter = getDelimiter();
		qs = new CsvQueryStruct();
		qs.setFilePath(fileLocation);
		qs.setDelimiter(delimiter);
		qs.setColumnTypes(dataTypes);
		qs.merge(this.qs);
		return qs;
	}

	/**************************************************************************************************
	 ************************************* INPUT METHODS***********************************************
	 **************************************************************************************************/

	private String getFileInfo() {
		GenRowStruct fGrs = this.store.getNoun(FILE_INFO);
		String fileInfo = null;
		if (fGrs != null && !fGrs.isEmpty()) {
			String encodedString = fGrs.get(0).toString();
			fileInfo = Utility.decodeURIComponent(encodedString);
		} else {
			throw new IllegalArgumentException("Need to specify " + FILE_INFO + "=[\"<encode>fileData</encode>\"] in pixel command");
		}
		return fileInfo;
	}

	private Map<String, String> getDataTypes() {
		GenRowStruct dataTypeGRS = this.store.getNoun(DATA_TYPES);
		Map<String, String> dataTypes = null;
		if (dataTypeGRS != null) {
			NounMetadata dataNoun = dataTypeGRS.getNoun(0);
			dataTypes = (Map<String, String>) dataNoun.getValue();
		}
		return dataTypes;
	}

	private char getDelimiter() {
		GenRowStruct delimGRS = this.store.getNoun(DELIMITER);
		String delimiter = "";
		char delim = ','; //default
		NounMetadata instanceIndexNoun;

		if (delimGRS != null) {
			instanceIndexNoun = delimGRS.getNoun(0);
			delimiter = (String) instanceIndexNoun.getValue();
		} else {
			throw new IllegalArgumentException("Need to specify " + DELIMITER + "=[delimiter] in pixel command");
		}

		//get char from input string
		if(delimiter.length() > 0) {
			delim = delimiter.charAt(0);
		}

		return delim;
	}
}