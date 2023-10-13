package prerna.reactor.algorithms.dataquality;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GetDQRulesReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(GetDQRulesReactor.class);
	private static final String DIR_SEP = System.getProperty("file.separator");
	
	public NounMetadata execute() {
		String base = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String fileLoc = base + DIR_SEP + "R" + DIR_SEP + "DQ" + DIR_SEP + "rule-defs.json";
		
		String fileString = null;
		try {
			fileString = FileUtils.readFileToString(new File(fileLoc));
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		Gson gson = new Gson();
		Map<String, Object> rulesMap = gson.fromJson(fileString, Map.class);
		NounMetadata noun = new NounMetadata(rulesMap, PixelDataType.MAP);
		return noun;
	}
}