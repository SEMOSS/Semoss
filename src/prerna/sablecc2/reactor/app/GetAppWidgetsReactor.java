package prerna.sablecc2.reactor.app;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GetAppWidgetsReactor extends AbstractReactor {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public GetAppWidgetsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		
		Map<String, Object> appWidgetsMap = new HashMap<String, Object>();
		
		final String basePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String appWidgetDirLoc = basePath + DIR_SEPARATOR + "db" + 
				DIR_SEPARATOR + appName + 
				DIR_SEPARATOR + "version" + 
				DIR_SEPARATOR + "widgets";
		
		File appWidgetDir = new File(appWidgetDirLoc);
		if(appWidgetDir.exists() && appWidgetDir.isDirectory()) {
			// we have the app folder
			// need to loop through and find the 
			File[] allFiles = appWidgetDir.listFiles();
			// each file should be a folder that contains widget information
			for(File widgetDir : allFiles) {
				// we need the directories
				if(widgetDir.isDirectory()) {
					// now find the config.json
					// that represents this widget
					File config = new File(widgetDir.getAbsolutePath() +  DIR_SEPARATOR + "config.json");
					if(config.exists()) {
						// this is what we want to send the FE
						Map<String, Object> mapData = null;
						try {
							mapData = new ObjectMapper().readValue(config, Map.class);
						} catch(Exception e) {
							e.printStackTrace();
							continue;
						}
						appWidgetsMap.put(widgetDir.getName(), mapData);
					}
				}
			}
		}
		
		return new NounMetadata(appWidgetsMap, PixelDataType.MAP, PixelOperationType.APP_WIDGETS);
	}
	
	
}
