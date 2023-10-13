package prerna.reactor.insights;

import java.util.Map;

import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PositionInsightRecipeStepReactor extends AbstractReactor {

	private static final String POSITION = "positionMap";
	
	public PositionInsightRecipeStepReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PIXEL_ID.getKey(), POSITION};
	}
	
	@Override
	public NounMetadata execute() {
		PixelList pixelList = this.insight.getPixelList();
		Pixel pixelObj = null;
		String pixelId = getPixelId();
		if(pixelId == null || pixelId.isEmpty()) {
			int size = pixelList.size();
			pixelObj = this.insight.getPixelList().get(size-1);
			pixelId = pixelObj.getId();
		}
		
		Map<String, Object> positionMap = getPosition();
		if(positionMap == null) {
			throw new NullPointerException("Must provide the position map");
		}
		
		if(pixelObj != null) {
			pixelObj.setPositionMap(positionMap);
		} else {
			pixelObj = pixelList.getPixel(pixelId);
			if(pixelObj != null) {
				pixelObj.setPositionMap(positionMap);
			} else {
				return getWarning("Unable to find pixelId = " + pixelId);
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Get the pixel id
	 * @return
	 */
	private String getPixelId() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0) + "";
		}
		
		if(!this.curRow.isEmpty()) {
			return this.curRow.get(0) + "";
		}
		
		return null;
	}
	
	/**
	 * Get the position map
	 * @return
	 */
	private Map<String, Object> getPosition() {
		GenRowStruct grs = this.store.getNoun(POSITION);
		if(grs != null && !grs.isEmpty()) {
			return (Map<String, Object>) grs.get(0);
		}
		return null;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(POSITION)) {
			return "The position map containing {'top'=[value1], 'left'=[value2]} where value1,value2 are integers";
		}
		return super.getDescriptionForKey(key);
	}
	
}
