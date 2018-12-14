package prerna.util.gson;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FilenameUtils;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.py.PandasFrame;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.om.InsightCacheUtility;
import prerna.om.InsightPanel;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelStreamUtility;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.om.task.TaskStore;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.translations.OptimizeRecipeTranslation;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.insight.InsightUtility;

public class InsightAdapter extends TypeAdapter<Insight> {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	// this var is only used so we have a way
	// to pass specific variables into a new insight we are creating from a cache
	// things like python thread
	// or potentially the full user object
	private Insight existingInsight;
	private ZipFile zip;
	private ZipOutputStream zos;
	
	/**
	 * Constructor for reading
	 * @param zip
	 */
	public InsightAdapter(ZipFile zip) {
		this.zip = zip;
	}
	
	/**
	 * Constructor for writing
	 * @param zos
	 */
	public InsightAdapter(ZipOutputStream zos) {
		this.zos = zos;
	}
	
	@Override
	public void write(JsonWriter out, Insight value) throws IOException {
		String rdbmsId = value.getRdbmsId();
		String engineId = value.getEngineId();
		String engineName = value.getEngineName();
		
		if(engineId == null || rdbmsId == null || engineName == null) {
			throw new IOException("Cannot jsonify an insight that is not saved");
		}
		
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folderDir = baseFolder + DIR_SEPARATOR + "db" + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) 
				+ DIR_SEPARATOR + "version" + DIR_SEPARATOR + rdbmsId;
		if(!(new File(folderDir).exists())) {
			new File(folderDir).mkdirs();
		}
		
		// start insight object
		out.beginObject();
		// write engine id
		out.name("engineId").value(engineId);
		// write engine name
		out.name("engineName").value(engineName);
		// write rdbms id
		out.name("rdbmsId").value(rdbmsId);
		
		// write varstore
		out.name("varstore");
		// output all variables that are not frames or tasks
		VarStoreAdapter varStoreAdapter = new VarStoreAdapter();
		VarStore varStore = value.getVarStore();
		varStoreAdapter.write(out, varStore);
		
		// consolidate frames due to alias
		// that point to the same frame
		List<FrameCacheHelper> frames = new Vector<FrameCacheHelper>();
		Set<String> keys = varStore.getKeys();
		for(String k : keys) {
			NounMetadata noun = varStore.get(k);
			PixelDataType type = noun.getNounType();
			if(type == PixelDataType.FRAME) {
				ITableDataFrame frame = (ITableDataFrame) noun.getValue();
				FrameCacheHelper existingFrameObject = findSameFrame(frames, frame);
				if(existingFrameObject != null) {
					existingFrameObject.addAlias(k);
				} else {
					FrameCacheHelper fObj = new FrameCacheHelper(frame);
					fObj.addAlias(k);
					frames.add(fObj);
				}
			}
		}
		
		// now that we have consolidated, write the frames
		out.name("frames");
		out.beginArray();
		for(FrameCacheHelper fObj : frames) {
			CachePropFileFrameObject saveFrame = null;
			try {
				saveFrame = fObj.frame.save(folderDir);
			} catch(Exception e) {
				e.printStackTrace();
				continue;
			}
			out.beginObject();
			out.name("file").value(parameterizePath(saveFrame.getFrameCacheLocation(), baseFolder, engineName, engineId));
			out.name("meta").value(parameterizePath(saveFrame.getFrameMetaCacheLocation(), baseFolder, engineName, engineId));
			out.name("state").value(parameterizePath(saveFrame.getFrameStateCacheLocation(), baseFolder, engineName, engineId));
			out.name("type").value(saveFrame.getFrameType());
			out.name("name").value(saveFrame.getFrameName());
			out.name("keys");
			out.beginArray();
			for(int i = 0; i < fObj.alias.size(); i++) {
				out.value(fObj.alias.get(i));
			}
			out.endArray();
			out.endObject();
			
			// add to zip
			File f1 = new File(saveFrame.getFrameCacheLocation());
			File f2 = new File(saveFrame.getFrameMetaCacheLocation());

			try {
				InsightCacheUtility.addToZipFile(f1, zos);
				InsightCacheUtility.addToZipFile(f2, zos);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		out.endArray();
		
		// write the panels
		out.name("panels");
		out.beginArray();
		Map<String, InsightPanel> panels = value.getInsightPanels();
		for(String key : panels.keySet()) {
			InsightPanel panel = panels.get(key);
			InsightPanelAdapter panelAdapter = new InsightPanelAdapter();
			panelAdapter.write(out, panel);
		}
		out.endArray();

		// write the tasks
		out.name("tasks");

		// i am also going to need
		// a panel id to task id mapping
		// which will be used for the json cache of the view
		TaskStore tStore = value.getTaskStore();
		TaskStoreAdapter tAdapter = new TaskStoreAdapter();
		tAdapter.write(out, tStore);
		List<Map<String, String>> panelIdToTaskList = tAdapter.getPanelIdToTask();
		
		// write the recipe
		List<String> recipe = value.getPixelRecipe();
		int steps = recipe.size();
		out.name("recipe");
		out.beginArray();
		for(int i = 0; i < steps; i++) {
			out.value(recipe.get(i));
		}
		out.endArray();
		
		// end insight object
		out.endObject();
				
		// write the json for the viz
		// this doesn't actually add anything to the insight object
		File vizOutputFile = new File(folderDir + DIR_SEPARATOR + InsightCacheUtility.VIEW_JSON);
		List<String> lastViewPixels = getLastViewPixels(recipe);
		Insight rerunInsight = new Insight();
		rerunInsight.setVarStore(value.getVarStore());
		rerunInsight.setUser(value.getUser());
		rerunInsight.setInsightPanels(value.getInsightPanels());
		PixelRunner pixelRunner = rerunInsight.runPixel(lastViewPixels);
		// i am going to need to go through
		// and re-align the task ids to match properly
		List<NounMetadata> pixelRunnerResults = pixelRunner.getResults();
		NOUN_LOOP : for(NounMetadata noun : pixelRunnerResults) {
			if(noun.getValue() instanceof ITask) {
				ITask t = (ITask) noun.getValue();
				TaskOptions taskOptions = t.getTaskOptions();
				if(taskOptions != null && !taskOptions.isEmpty()) {
					Set<String> panelIds = taskOptions.getPanelIds();
					for(String panelId : panelIds) {
						for(int i = 0; i < panelIdToTaskList.size(); i++) {
							// this panel to id is always size 1!!!
							Map<String, String> panelIdToTask  = panelIdToTaskList.get(i);
							if(panelIdToTask.containsKey(panelId)) {
								t.setId(panelIdToTask.get(panelId));
								// each index only has 1 panel to id
								// so we can just drop it now
								panelIdToTaskList.remove(i);
								continue NOUN_LOOP;
							}
						}
					}
				}
			}
		}
		// now that we have updated the task ids
		// lets write it
		PixelStreamUtility.writePixelData(pixelRunner, vizOutputFile, false);
		
		// add it to the zip
		try {
			InsightCacheUtility.addToZipFile(vizOutputFile, zos);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private FrameCacheHelper findSameFrame(List<FrameCacheHelper> frames, ITableDataFrame frame) {
		int size = frames.size();
		for(int i = 0; i < size; i++) {
			if(frames.get(i).sameFrame(frame)) {
				return frames.get(i);
			}
		}
		
		return null;
	}
	
	private List<String> getLastViewPixels(List<String> recipe) {
		OptimizeRecipeTranslation translation = new OptimizeRecipeTranslation();
		for (int i = 0; i < recipe.size(); i++) {
			String expression = recipe.get(i);
			// fill in the encodedToOriginal with map for the current expression
			expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodedToOriginal);
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation
				// when we apply the translation, we will change encoded expressions back to their original form
				tree.apply(translation);
				// reset translation.encodedToOriginal for each expression
				translation.encodedToOriginal = new HashMap<String, String>();
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		// we want to run the finalizeExpressionsToKeep method only after all expressions have been run
		// this way we can find the last expression index used 
		translation.finalizeExpressionsToKeep();
		return translation.getLastTasksForPanels();
	}
	
	
	@Override
	public Insight read(JsonReader in) throws IOException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);

		Insight insight = new Insight();
		InsightUtility.transferDefaultVars(existingInsight, insight);
		
		in.beginObject();
		in.nextName();
		// engine id, engine name, rdbms id
		String engineId = in.nextString();
		insight.setEngineId(engineId);
		in.nextName();
		String engineName = in.nextString();
		insight.setEngineName(engineName);
		in.nextName();
		String rdbmsId = in.nextString();
		insight.setRdbmsId(rdbmsId);
		
		// this will be the varstore
		in.nextName();
		VarStoreAdapter varStoreAdapter = new VarStoreAdapter();
		VarStore store = varStoreAdapter.read(in);
		insight.setVarStore(store);
		
		// this will be the frames
		in.nextName();
		in.beginArray();
		while(in.hasNext()) {
			in.beginObject();
			
			List<String> varStoreKeys = new Vector<String>();
			CachePropFileFrameObject cf = new CachePropFileFrameObject();
			while(in.hasNext()) {
				String k = in.nextName();
				if(k.equals("file")) {
					String path = deparameterizePath(in.nextString(), baseFolder, engineName, engineId);
					if(!(new File(path).exists())) {
						InsightCacheUtility.unzipFile(zip, FilenameUtils.getName(path), path);
					}
					cf.setFrameCacheLocation(path);
				} else if(k.equals("meta")) {
					String path = deparameterizePath(in.nextString(), baseFolder, engineName, engineId);
					if(!(new File(path).exists())) {
						InsightCacheUtility.unzipFile(zip, FilenameUtils.getName(path), path);
					}
					cf.setFrameMetaCacheLocation(path);
				} else if(k.equals("type")) {
					cf.setFrameType(in.nextString());
				} else if(k.equals("name")) {
					cf.setFrameName(in.nextString());
				} else if(k.equals("state")) {
					// this is not always present
					JsonToken peek = in.peek();
					if(peek == JsonToken.NULL) {
						in.nextNull();
					} else {
						String path = deparameterizePath(in.nextString(), baseFolder, engineName, engineId);
						if(!(new File(path).exists())) {
							InsightCacheUtility.unzipFile(zip, FilenameUtils.getName(path), path);
						}
						cf.setFrameStateCacheLocation(path);
					}
				} else if(k.equals("keys")) {
					in.beginArray();
					while(in.hasNext()) {
						varStoreKeys.add(in.nextString());
					}
					in.endArray();
				}
			}

			ITableDataFrame frame;
			try {
				frame = (ITableDataFrame) Class.forName(cf.getFrameType()).newInstance();
				// need to set the exector for pandas
				if(frame instanceof PandasFrame) {
					((PandasFrame)frame).setJep(insight.getPy());
				}
				frame.open(cf);
				
				NounMetadata fNoun = new NounMetadata(frame, PixelDataType.FRAME);
				for(String varStoreK : varStoreKeys) {
					store.put(varStoreK, fNoun);
				}
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			
			in.endObject();
		}
		in.endArray();
		
		// this will be the panels
		in.nextName();
		in.beginArray();
		while(in.hasNext()) {
			InsightPanelAdapter panelAdapter = new InsightPanelAdapter();
			InsightPanel panel = panelAdapter.read(in);
			insight.addNewInsightPanel(panel);
		}
		in.endArray();
		
		// this will be the tasks
		in.nextName();
		TaskStoreAdapter tStoreAdapter = new TaskStoreAdapter();
		TaskStore tStore = tStoreAdapter.read(in);
		insight.setTaskStore(tStore);
		
		// this will be the recipe
		in.nextName();
		List<String> recipe = new Vector<String>();
		in.beginArray();
		while(in.hasNext()) {
			recipe.add(in.nextString());
		}
		in.endArray();
		insight.setPixelRecipe(recipe);
		
		in.endObject();
		return insight;
	}

	private static String parameterizePath(String path, String baseFolder, String engineName, String engineId) {
		if(path == null) {
			return null;
		}
		path = path.replace(baseFolder, "@" + Constants.BASE_FOLDER + "@");
		path = path.replace(SmssUtilities.getUniqueName(engineName, engineId), "@" + Constants.ENGINE + "@");
		return path;
	}
	
	private static String deparameterizePath(String path, String baseFolder, String engineName, String engineId) {
		if(path == null) {
			return null;
		}
		path = path.replace("@" + Constants.BASE_FOLDER + "@", baseFolder);
		path = path.replace("@" + Constants.ENGINE + "@", SmssUtilities.getUniqueName(engineName, engineId));
		return path;
	}

	public void setUserContext(Insight existingInsight) {
		this.existingInsight = existingInsight;		
	}
	
}

/**
 * Simple object to help cache frames
 */
class FrameCacheHelper {
	
	ITableDataFrame frame;
	List<String> alias = new Vector<String>();
	
	FrameCacheHelper(ITableDataFrame frame) {
		this.frame = frame;
	}
	
	public void addAlias(String alias) {
		this.alias.add(alias);
	}
	
	public boolean sameFrame(ITableDataFrame frame) {
		return this.frame == frame;
	}
}

