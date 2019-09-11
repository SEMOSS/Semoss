package prerna.sablecc2.reactor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PixelSourceReactor extends AbstractReactor {

	public PixelSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.IN_APP.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		
		
		boolean app = (keyValue.containsKey(keysToGet[1]) && keyValue.get(keysToGet[1]).equalsIgnoreCase("app")) ;//|| (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets"));
		boolean isUser = (keyValue.containsKey(keysToGet[1]) && keyValue.get(keysToGet[1]).equalsIgnoreCase("user")) ;

		String assetFolder = this.insight.getInsightFolder();
		if(isUser)
		{
			// do other things
		}
		if (app) {
			assetFolder = this.insight.getAppFolder();
		}

		
		String path = assetFolder + DIR_SEPARATOR + relativePath;

		// read in the file
		// execute it within this insight
		// return the results
		
		File file = new File(path);
		if(!file.exists()) {
			throw new IllegalArgumentException("Could not find the file path : " + relativePath);
		}
		
		String pixel = null;
		try {
			pixel = FileUtils.readFileToString(file);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Issue occured properly reading file", e);
		}
		
		if(pixel == null || (pixel = pixel.trim()).isEmpty()) {
			throw new IllegalArgumentException("Pixel file is empty");
		}
		
		PixelRunner pixelReturn = this.insight.runPixel(pixel);
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", pixelReturn);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER);
		return noun;
	}
}
