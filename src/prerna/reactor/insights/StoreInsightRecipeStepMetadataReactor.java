package prerna.reactor.insights;

import java.util.HashMap;
import java.util.Map;

import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StoreInsightRecipeStepMetadataReactor extends AbstractReactor {

	public StoreInsightRecipeStepMetadataReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PIXEL_ID.getKey(), ReactorKeysEnum.ALIAS.getKey(), ReactorKeysEnum.DESCRIPTION.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String pixelId = this.keyValue.get(this.keysToGet[0]);
		String alias = this.keyValue.get(this.keysToGet[1]);
		String description = this.keyValue.get(this.keysToGet[2]);
		
		PixelList pixelList = this.insight.getPixelList();
		Pixel pixelObj = null;
		if(pixelId == null || pixelId.isEmpty()) {
			int size = pixelList.size();
			pixelObj = this.insight.getPixelList().get(size-1);
			pixelId = pixelObj.getId();
		}
		
		if(pixelObj != null) {
			pixelObj.setPixelAlias(alias);
			pixelObj.setPixelDescription(description);
		} else {
			pixelObj = pixelList.getPixel(pixelId);
			if(pixelObj != null) {
				pixelObj.setPixelAlias(alias);
				pixelObj.setPixelDescription(description);
			} else {
				return getWarning("Unable to find pixelId = " + pixelId);
			}
		}
		
		Map<String, String> newValues = new HashMap<>();
		newValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), pixelId);
		newValues.put(ReactorKeysEnum.ALIAS.getKey(), alias);
		newValues.put(ReactorKeysEnum.DESCRIPTION.getKey(), description);
		return new NounMetadata(newValues, PixelDataType.MAP);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.ALIAS.getKey())) {
			return "The alias to assign for the pixel recipe step";
		} else if(key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
			return "The description to provide for the pixel recipe step";
		}
		return super.getDescriptionForKey(key);
	}
	
}
