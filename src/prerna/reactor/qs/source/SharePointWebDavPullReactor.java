
package prerna.reactor.qs.source;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SharePointWebDavPullReactor extends AbstractQueryStructReactor{

	//private String[] keysToGet;
	private static final String CLASS_NAME = SharePointWebDavPullReactor.class.getName();


	public SharePointWebDavPullReactor() {
		this.keysToGet = new String[] { "path" };
	}

	@Override
	protected SelectQueryStruct createQueryStruct() {

		//get keys
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String filePath = this.keyValue.get(this.keysToGet[0]);
		if (filePath == null || filePath.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file path");
		}


		String filePathDest = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		filePathDest += "\\" + Utility.getRandomString(10) + ".csv";
		filePathDest = filePathDest.replace("\\", "/");
		try {
			File source = new File(filePath);
			File destination = new File(filePathDest);
			FileUtils.copyFile(source, destination);
		} catch (IOException e1) {
		
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// get datatypes
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(filePathDest);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
		Map<String, String> dataTypes = predictionMaps[0];
		Map<String, String> additionalDataTypes = predictionMaps[1];
		CsvQueryStruct qs = new CsvQueryStruct();
		for (String key : dataTypes.keySet()) {
			qs.addSelector("DND", key);
		}
		helper.clear();
		qs.merge(this.qs);
		qs.setFilePath(filePathDest);
		qs.setDelimiter(',');
		qs.setColumnTypes(dataTypes);
		qs.setAdditionalTypes(additionalDataTypes);
		return qs;


	}


}