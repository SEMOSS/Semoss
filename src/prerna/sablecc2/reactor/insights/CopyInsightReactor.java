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
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.InsightCacheUtility;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
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
		 * We do this by caching the current insight into a folder
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
				
				// TODO: need to better perform this logic
				// since we are copying a base insight for preview
				// and we delete a file that was uploaded to the server
				// things will break
				// need to come back for when/if we use this for other things
				// aside from preview
				in.setDeleteFilesOnDropInsight(false);
				in.setDeleteREnvOnDropInsight(false);

				// since R and Py share the same user space
				// i will need to go through and modify them to have another variable name
				VarStore vStore = in.getVarStore();
				Set<String> varKeys = vStore.getKeys();
				List<ITableDataFrame> copiedAlready = new Vector<ITableDataFrame>();
				for(String var : varKeys) {
					NounMetadata variable = vStore.get(var);
					if(variable.getNounType() == PixelDataType.FRAME) {
						// if R or Py, we have to change
						// update the name of the table + update the metadata
						Object o = variable.getValue();
						if(copiedAlready.contains(o)) {
							continue;
						}
						if(o instanceof RDataTable) {
							RDataTable dt = (RDataTable) variable.getValue();
							String oldName = dt.getName();
							String newName = oldName + "_COPY";
							dt.executeRScript(newName + "<- " + oldName);
							dt.setName(newName);
							dt.getMetaData().modifyVertexName(oldName, newName);
							// store so we dont do this multiple times if frame is referenced more than once
							copiedAlready.add(dt);
						} else if(o instanceof PandasFrame) {
							PandasFrame dt = (PandasFrame) variable.getValue();
							String oldName = dt.getName();
							String newName = oldName + "_COPY";
							dt.runScript(newName + " = " + oldName + ".copy(deep=True)");
							// also do the wrapper
							dt.setName(newName);
							// the wrapper name is auto generated when you set name
							String newWrapperName = dt.getWrapperName();
							dt.runScript(PandasSyntaxHelper.makeWrapper(newWrapperName, newName));
							dt.getMetaData().modifyVertexName(oldName, newName);
							// store so we dont do this multiple times if frame is referenced more than once
							copiedAlready.add(dt);
						}
					}
				}
				
				// drop the insight folder
				insightFile.delete();
				
				// need to set the new insight with a new id
				in.setInsightId(UUID.randomUUID().toString());
				// need to set the paths so it can access all the same assets
				in.setInsightFolder(this.insight.getInsightFolder());
				in.setAppFolder(this.insight.getAppFolder());
				InsightStore.getInstance().put(in);
				
				List<String> recipe = new ArrayList<String>();
				try {
					String[] recipeToRun = getRecipe();
					recipeToRun = decodeRecipe(recipeToRun);
					recipe.addAll(Arrays.asList(recipeToRun));
				} catch(IllegalArgumentException e) {
					// ignore
				}
				if(dropInsight()) {
					recipe.add("DropInsight();");
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