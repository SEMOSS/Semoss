package prerna.reactor.insights.themes;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;

public class ReadInsightThemeReactor extends AbstractInsightReactor {
	
	private static final Logger classLogger = LogManager.getLogger(ReadInsightThemeReactor.class);

	@Override
	public NounMetadata execute() {
		String versionFilePath = AssetUtility.getAssetBasePath(this.insight, null, false);
		String insightThemeFilePath = versionFilePath + DIR_SEPARATOR + IMAGE_THEME_FILE;
		File insightThemeFile = new File(insightThemeFilePath);
		if(!insightThemeFile.exists() && !insightThemeFile.isFile()) {
			return new NounMetadata(new HashMap<>(), PixelDataType.MAP, PixelOperationType.INSIGHT_THEME);
		}
		
		Map<String, Object> value = null;
		try(Reader reader = new FileReader(insightThemeFile)) {
			Gson gson = new Gson();
			value = gson.fromJson(reader, Map.class);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("An error occurred trying to read the insight theme");
		}
		
		return new NounMetadata(value, PixelDataType.MAP, PixelOperationType.INSIGHT_THEME);
	}

}
