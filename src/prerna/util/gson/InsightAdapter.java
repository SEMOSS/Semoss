package prerna.util.gson;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import javax.crypto.Cipher;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.CachePropFileFrameObject;
import prerna.cache.InsightCacheUtility;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.om.PixelList;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelStreamUtility;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskStore;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class InsightAdapter extends TypeAdapter<Insight> {

	private static final Logger classLogger = LogManager.getLogger(InsightAdapter.class);

	private static final String CLASS_NAME = InsightAdapter.class.getName();
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	// this var is only used so we have a way
	// to pass specific variables into a new insight we are creating from a cache
	// things like python thread
	// or potentially the full user object
	private Insight existingInsight;
	private ZipFile zip;
	
	private ZipOutputStream zos = null;
	private String folderDir = null;
	private boolean encrypt = false;
	private Cipher cipher = null;
	
	private Set<String> varsToExclude;

	/**
	 * Constructor for reading
	 * @param zip
	 */
	public InsightAdapter(ZipFile zip) {
		this.zip = zip;
		this.varsToExclude = new HashSet<>();
	}
	
	/**
	 * Constructor for writing
	 * @param folderDir
	 * @param zos
	 */
	public InsightAdapter(String folderDir, ZipOutputStream zos) {
		this.folderDir = folderDir;
		this.zos = zos;
		this.varsToExclude = new HashSet<>();
	}
	
	@Override
	public void write(JsonWriter out, Insight value) throws IOException {
		String rdbmsId = value.getRdbmsId();
		String projectId = value.getProjectId();
		String projectName = value.getProjectName();
		
		if(projectId == null || rdbmsId == null || projectName == null) {
			throw new IOException("Cannot jsonify an insight that is not saved");
		}

		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		if(!(new File(Utility.normalizePath(this.folderDir)).exists())) {
			new File(Utility.normalizePath(this.folderDir)).mkdirs();
		}
		
		// start insight object
		out.beginObject();
		// write engine id
		out.name("projectId").value(projectId);
		// write engine name
		out.name("projectName").value(projectName);
		// write rdbms id
		out.name("rdbmsId").value(rdbmsId);
		
		// write varstore
		// it is important that we write the first
		// since we use this on read for references to frames
		out.name("varstore");
		// output all variables that are not frames or tasks
		VarStoreAdapter varStoreAdapter = new VarStoreAdapter();
		varStoreAdapter.setKeysToIgnore(this.varsToExclude);
		varStoreAdapter.setCollectFrames(true);
		VarStore varStore = value.getVarStore();
		varStoreAdapter.write(out, varStore);
		
		// for optimization
		// we collected the frames during the above adapter writing
		// it also ignores the keys based on varsToExclude
		List<FrameCacheHelper> frames = varStoreAdapter.getFrames();
		
		// now that we have consolidated, write the frames
		out.name("frames");
		out.beginArray();
		for(FrameCacheHelper fObj : frames) {
			// set the logger for this frame
			fObj.getFrame().setLogger(classLogger);
			CachePropFileFrameObject saveFrame = fObj.getFrame().save(this.folderDir, this.cipher);
			out.beginObject();
			out.name("file").value(parameterizePath(saveFrame.getFrameCacheLocation(), baseFolder, projectName, projectId));
			out.name("meta").value(parameterizePath(saveFrame.getFrameMetaCacheLocation(), baseFolder, projectName, projectId));
			out.name("state").value(parameterizePath(saveFrame.getFrameStateCacheLocation(), baseFolder, projectName, projectId));
			out.name("type").value(saveFrame.getFrameType());
			out.name("name").value(saveFrame.getFrameName());
			out.name("keys");
			out.beginArray();
			List<String> alias = fObj.getAlias();
			for(int i = 0; i < alias.size(); i++) {
				out.value(alias.get(i));
			}
			out.endArray();
			out.endObject();
			
			// add to zip
			File f1 = new File(Utility.normalizePath(saveFrame.getFrameCacheLocation()));
			File f2 = new File(Utility.normalizePath(saveFrame.getFrameMetaCacheLocation()));

			try {
				InsightCacheUtility.addToZipFile(f1, zos);
				InsightCacheUtility.addToZipFile(f2, zos);
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		out.endArray();
		
		// write the sheets
		out.name("sheets");
		out.beginArray();
		Map<String, InsightSheet> sheets = value.getInsightSheets();
		for(String key : sheets.keySet()) {
			InsightSheet sheet = sheets.get(key);
			InsightSheetAdapter sheetAdapter = new InsightSheetAdapter();
			sheetAdapter.write(out, sheet);
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

		// this is not written
		// but need to store the refresh panel task ids
		// get the last task at each layer for each panel
		// this will be written to the vizOutputFile
		Insight cachedInsight = new Insight();
		// TODO: this is because the BE adds a default sheet for new insights
		// but this is not required for cached insights
		cachedInsight.getInsightSheets().clear();
		InsightUtility.transferDefaultVars(value, cachedInsight);
		cachedInsight.setVarStore(value.getVarStore());
		cachedInsight.setUser(value.getUser());
		cachedInsight.setInsightSheets(value.getInsightSheets());
		cachedInsight.setInsightPanels(value.getInsightPanels());
		// make sure the task ids do not overlap
		cachedInsight.setTaskStore(value.getTaskStore());
		List<String> cachedRecipe = PixelUtility.getCachedInsightRecipe(value);
		PixelRunner pixelRunner = cachedInsight.runPixel(cachedRecipe);
		
		// write the tasks
		out.name("tasks");
		TaskStore tStore = value.getTaskStore();
		TaskStoreAdapter tAdapter = new TaskStoreAdapter();
		tAdapter.write(out, tStore);
		
		// write the recipe
		PixelList pixelList = value.getPixelList();
		out.name("recipe");
		PixelListAdapter pAdapter = new PixelListAdapter();
		pAdapter.write(out, pixelList);
		
		// end insight object
		out.endObject();
		
		// this is no longer part of the actual 
		// insight serialization
		// but writing things to disk
		
		// write the json for the viz
		// this doesn't actually add anything to the insight object
		File vizOutputFile = new File(Utility.normalizePath(this.folderDir) + DIR_SEPARATOR + InsightCacheUtility.VIEW_JSON);
		// lets write it
		PixelStreamUtility.writePixelData(pixelRunner, vizOutputFile, this.cipher);
		
		// add it to the zip
		try {
			InsightCacheUtility.addToZipFile(vizOutputFile, zos);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}
	
	@Override
	public Insight read(JsonReader in) throws IOException {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);

		Insight insight = new Insight();
		// TODO: this is because the BE adds a default sheet for new insights
		// but this is not required for cached insights
		insight.getInsightSheets().clear();
		
		in.beginObject();
		in.nextName();
		// engine id, engine name, rdbms id
		String projectId = in.nextString();
		insight.setProjectId(projectId);
		in.nextName();
		String projectName = in.nextString();
		insight.setProjectName(projectName);
		in.nextName();
		String rdbmsId = in.nextString();
		insight.setRdbmsId(rdbmsId);
		
		// this will be the varstore
		in.nextName();
		VarStoreAdapter varStoreAdapter = new VarStoreAdapter();
		VarStore store = varStoreAdapter.read(in);
		if(store != null) {
			insight.setVarStore(store);
		}
		if(this.existingInsight != null) {
			InsightUtility.transferDefaultVars(this.existingInsight, insight);
		}
		
		// this will be the frames
		in.nextName();
		in.beginArray();
		while(in.hasNext()) {
			in.beginObject();
			
			List<String> varStoreKeys = new Vector<>();
			CachePropFileFrameObject cf = new CachePropFileFrameObject();
			while(in.hasNext()) {
				String k = in.nextName();
				if(k.equals("file")) {
					String path = deparameterizePath(in.nextString(), baseFolder, projectName, projectId);
					String normalizedPath = Utility.normalizePath(path);

					if(!(new File(normalizedPath).exists())) {
						InsightCacheUtility.unzipFile(zip, FilenameUtils.getName(normalizedPath), normalizedPath);
					}
					cf.setFrameCacheLocation(path);
				} else if(k.equals("meta")) {
					String path = deparameterizePath(in.nextString(), baseFolder, projectName, projectId);
					String normalizedPath = Utility.normalizePath(path);

					if(!(new File(normalizedPath).exists())) {
						InsightCacheUtility.unzipFile(zip, FilenameUtils.getName(normalizedPath), normalizedPath);
					}
					cf.setFrameMetaCacheLocation(normalizedPath);
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
						String path = deparameterizePath(in.nextString(), baseFolder, projectName, projectId);
						String normalizedPath = Utility.normalizePath(path);

						if(!(new File(normalizedPath).exists())) {
							InsightCacheUtility.unzipFile(zip, FilenameUtils.getName(normalizedPath), normalizedPath);
						}
						cf.setFrameStateCacheLocation(normalizedPath);
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
				String className = cf.getFrameType();
				frame = (ITableDataFrame) Class.forName(className).newInstance();
				// need to set the exector for pandas
				if(frame instanceof PandasFrame) {
					((PandasFrame)frame).setJep(insight.getPy());
					((PandasFrame)frame).setTranslator(insight.getPyTranslator());
				}
				else if(frame instanceof RDataTable) {
					frame = new RDataTable(insight.getRJavaTranslator(CLASS_NAME));
				}
				
				frame.open(cf, this.cipher);
				
				NounMetadata fNoun = new NounMetadata(frame, PixelDataType.FRAME);
				for(String varStoreK : varStoreKeys) {
					store.put(varStoreK, fNoun);
				}
			} catch (InstantiationException e) {
				classLogger.error(Constants.STACKTRACE, e);
			} catch (IllegalAccessException iae) {
				classLogger.error(Constants.STACKTRACE, iae);
			} catch (ClassNotFoundException cnfe) {
				classLogger.error(Constants.STACKTRACE, cnfe);
			}
			
			in.endObject();
		}
		in.endArray();

		// this will be the sheets
		// need to account for legacy
		String sheetKey = in.nextName();
		if(sheetKey.equals("sheets")) {
			in.beginArray();
			while(in.hasNext()) {
				InsightSheetAdapter sheetAdapter = new InsightSheetAdapter();
				sheetAdapter.setInsight(insight);
				InsightSheet sheet = sheetAdapter.read(in);
				insight.addNewInsightSheet(sheet);
			}
			in.endArray();
			
			// this will be the panels
			in.nextName();
			in.beginArray();
			while(in.hasNext()) {
				InsightPanelAdapter panelAdapter = new InsightPanelAdapter();
				panelAdapter.setInsight(insight);
				InsightPanel panel = panelAdapter.read(in);
				insight.addNewInsightPanel(panel);
			}
			in.endArray();
		} else {
			// this is legacy where we only have panels and no sheets
			// just load the sheets
			in.beginArray();
			while(in.hasNext()) {
				InsightPanelAdapter panelAdapter = new InsightPanelAdapter();
				panelAdapter.setInsight(insight);
				InsightPanel panel = panelAdapter.read(in);
				insight.addNewInsightPanel(panel);
			}
			in.endArray();
		}
		
		// this will be the tasks
		in.nextName();
		TaskStoreAdapter tStoreAdapter = new TaskStoreAdapter();
		tStoreAdapter.setInsight(insight);
		TaskStore tStore = tStoreAdapter.read(in);
		insight.setTaskStore(tStore);
		
		// this will be the recipe
		in.nextName();
		PixelListAdapter pAdapter = new PixelListAdapter();
		pAdapter.setInsight(insight);
		PixelList pixelList = pAdapter.read(in);
		insight.setPixelList(pixelList);
		
		in.endObject();
		return insight;
	}

	private static String parameterizePath(String path, String baseFolder, String engineName, String engineId) {
		if(path == null) {
			return null;
		}
		
		// we want to keep the final / in the path
		if(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) {
			baseFolder = baseFolder.substring(0, baseFolder.length()-1);
		}
		
		path = path.replace(baseFolder, "@" + Constants.BASE_FOLDER + "@");
		path = path.replace(SmssUtilities.getUniqueName(engineName, engineId), "@" + Constants.ENGINE + "@");
		return path;
	}
	
	private static String deparameterizePath(String path, String baseFolder, String engineName, String engineId) {
		if(path == null) {
			return null;
		}
		
		// if we have a separator in the parameterized path
		// we do not want a separator in baseFolder
		// if there is no separator in the parameterized path
		// then we do want a separator in baseFolder
		
		// separator parameterized path 
		if(path.contains("@" + Constants.BASE_FOLDER + "@\\") || path.contains("@" + Constants.BASE_FOLDER + "@/")) {
			if(baseFolder.endsWith("/") || baseFolder.endsWith("\\")) {
				baseFolder = baseFolder.substring(0, baseFolder.length()-1);
			}
		}
		// no separator in parameterized path
		else {
			if(!baseFolder.endsWith("/") && !baseFolder.endsWith("\\")) {
				baseFolder += DIR_SEPARATOR;
			}
		}
		
		path = path.replace("@" + Constants.BASE_FOLDER + "@", baseFolder);
		path = path.replace("@" + Constants.ENGINE + "@", SmssUtilities.getUniqueName(engineName, engineId));
		return path;
	}

	public void setUserContext(Insight existingInsight) {
		this.existingInsight = existingInsight;		
	}
	
	public void setVarsToExclude(Set<String> varsToExclude) {
		this.varsToExclude = varsToExclude;
	}

	public boolean isEncrypt() {
		return encrypt;
	}

	public void setEncrypt(boolean encrypt) {
		this.encrypt = encrypt;
	}

	public Cipher getCipher() {
		return cipher;
	}

	public void setCipher(Cipher cipher) {
		this.cipher = cipher;
	}
	
}
