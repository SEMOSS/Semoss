package prerna.reactor.insights.recipemanagement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteInsightRecipeStepReactor extends AbstractReactor {

	private static final String CLASS_NAME = DeleteInsightRecipeStepReactor.class.getName();

	private static final String PROPAGATE = "propagate";
	
	public DeleteInsightRecipeStepReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PIXEL_ID.getKey(), PROPAGATE};
	}
	
	@Override
	public NounMetadata execute() {
		List<String> pixelIds = getPixelIds();
		if(pixelIds == null || pixelIds.isEmpty()) {
			throw new NullPointerException("Pixel ids to remove cannot be null or empty");
		}
		// takes into consideration deleting downstream recipe steps
		boolean propagate = propagate();
		Logger logger = getLogger(CLASS_NAME);

		// grab the pixel list
		// and remove the ids
		PixelList pixelList = this.insight.getPixelList();
		pixelList.removeIds(pixelIds, propagate);
		
		// now i need to rerun the insight recipe
		// clear the insight
		// and re-run it
		logger.info("Re-executing the insight recipe... please wait as this operation may take some time");
		PixelRunner runner = this.insight.reRunPixelInsight(false);
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.RERUN_INSIGHT_RECIPE);
		return noun;
	}

	/**
	 * Get the list of ids
	 * @return
	 */
	private List<String> getPixelIds() {
		List<String> pixelIds = new Vector<>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				pixelIds.add(grs.get(i) + "");
			}
			return pixelIds;
		}
		
		if(!this.curRow.isEmpty()) {
			for(int i = 0; i < this.curRow.size(); i++) {
				pixelIds.add(this.curRow.get(i) + "");
			}
		}
		
		return pixelIds;
	}
	
	/**
	 * Propagate the deletion
	 * @return
	 */
	private boolean propagate() {
		GenRowStruct grs = this.store.getNoun(PROPAGATE);
		if(grs != null && grs.isEmpty()) {
			return Boolean.parseBoolean(grs.get(0).toString());
		}
		// default is true
		return true;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(PROPAGATE)) {
			return "Propagate the deletion to all child steps that utilize the frames for this step";
		}
		return super.getDescriptionForKey(key);
	}
	
}
