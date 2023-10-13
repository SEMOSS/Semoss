package prerna.reactor;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class PixelSourceReactor extends AbstractReactor {

	public PixelSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);
		String path = assetFolder + DIR_SEPARATOR + relativePath;

		// read in the file
		// execute it within this insight
		// return the results
		File file = new File(Utility.normalizePath(path));
		if(!file.exists()) {
			throw new IllegalArgumentException("Could not find the file path : " + relativePath);
		}
		
		String pixel = null;
		try {
			pixel = FileUtils.readFileToString(file);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Issue occurred properly reading file", e);
		}
		
		if(pixel == null || (pixel = pixel.trim()).isEmpty()) {
			throw new IllegalArgumentException("Pixel file is empty");
		}
		
		PixelRunner pixelReturn = this.insight.runPixel(pixel);
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", pixelReturn);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.SUB_SCRIPT);
		return noun;
	}
}
