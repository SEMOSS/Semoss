package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.om.Insight;
import prerna.om.InsightCacheUtility;
import prerna.om.InsightStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.gson.UnsavedInsightAdapter;

public class CopyInsightReactor extends AbstractInsightReactor {
	
	public static final String DROP_INSIGHT = "drop";
	
	public CopyInsightReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.RECIPE.getKey(), DROP_INSIGHT};
	}
	
	@Override
	public NounMetadata execute() {
		/*
		 * Create a new empty insight
		 * We do this by cahing the current insight into a folder
		 * And then reading that cache 
		 * Will override the insight id to be a new one
		 */
		
		// get the directory to write to
		
		String folderDirLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) 
				+ DIR_SEPARATOR + "ICache_" + Utility.getRandomString(6);
		
		File folderDir = new File(folderDirLoc);
		if(!folderDir.exists()) {
			folderDir.mkdirs();
		}
		
		String insightLoc = null;
		File insightFile = null;
		{
			UnsavedInsightAdapter adapter = new UnsavedInsightAdapter(folderDir);
			StringWriter writer = new StringWriter();
			JsonWriter jWriter = new JsonWriter(writer);
			try {
				adapter.write(jWriter, this.insight);
				insightLoc = folderDir+ DIR_SEPARATOR + InsightCacheUtility.MAIN_INSIGHT_JSON;
				insightFile = new File(insightLoc);
				FileUtils.writeStringToFile(insightFile, writer.toString());
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("An error occured trying to copy the insight");
			}
		}
		try {
			// now we need to read that insight cache
			UnsavedInsightAdapter adapter = new UnsavedInsightAdapter(folderDir);
			StringReader reader = new StringReader(FileUtils.readFileToString(insightFile));
			JsonReader jReader = new JsonReader(reader);
				// set the user context for reading
				// need this to pass default insight parameters + user
				adapter.setUserContext(this.insight);
				Insight in = adapter.read(jReader);
				
				// need to set the new insight with a new id
				in.setInsightId(UUID.randomUUID().toString());
				InsightStore.getInstance().putWithCurrentId(in);
				
				List<String> recipe = new ArrayList<String>();
				try {
					String[] recipeToRun = getRecipe();
					recipe.addAll(Arrays.asList(recipeToRun));
					if(dropInsight()) {
						recipe.add("DropInsight();");
					}
				} catch(IllegalArgumentException e) {
					// ignore
				}
				Map<String, Object> runnerWraper = new HashMap<String, Object>();
				runnerWraper.put("runner", in.runPixel(recipe));
				NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.NEW_EMPTY_INSIGHT);
				return noun;
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("An error occured trying to read the insight copy");
		}
	}
	
	/**
	 * Determine if we should drop the insight at the end
	 * @return
	 */
	private boolean dropInsight() {
		GenRowStruct grs = this.store.getNoun(DROP_INSIGHT);
		if(grs != null && !grs.isEmpty()) {
			return (boolean) grs.get(0);
		}
		
		return true;
	}
	
}