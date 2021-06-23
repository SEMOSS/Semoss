package prerna.sablecc2.reactor.insights.themes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.AssetUtility;

public class SetInsightThemeReactor extends AbstractInsightReactor {

	@Override
	public NounMetadata execute() {
		String versionFilePath = AssetUtility.getAssetVersionBasePath(this.insight, null, true);
		String insightThemeFilePath = versionFilePath + DIR_SEPARATOR + IMAGE_THEME_FILE;
		File insightThemeFile = new File(insightThemeFilePath);
		if(insightThemeFile.exists() && insightThemeFile.isFile()) {
			// delete the current one
			insightThemeFile.delete();
		}
		
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs.isEmpty()) {
			throw new SemossPixelException("No insight theme json was passed to save");
		}
		
		Map<String, Object> value = (Map<String, Object>) mapInputs.get(0).getValue();
		try(Writer writer = new FileWriter(insightThemeFile)) {
			Gson gson = new Gson();
			gson.toJson(value, writer);
		} catch (IOException e) {
			e.printStackTrace();
			throw new SemossPixelException("An error occured trying to save the insight theme");
		}
		
		return new NounMetadata(value, PixelDataType.MAP, PixelOperationType.INSIGHT_THEME);
	}

}
