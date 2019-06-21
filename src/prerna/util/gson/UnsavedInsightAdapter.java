package prerna.util.gson;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.CachePropFileFrameObject;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.impl.r.RserveUtil;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskStore;
import prerna.util.insight.InsightUtility;

public class UnsavedInsightAdapter extends TypeAdapter<Insight> {

	private File folderDir;
	private Insight existingInsight;
	
	/**
	 * Constructor 
	 * @param zip
	 */
	public UnsavedInsightAdapter(File f) {
		this.folderDir = f;
	}
	
	@Override
	public void write(JsonWriter out, Insight value) throws IOException {
		String rdbmsId = value.getRdbmsId();
		String engineId = value.getEngineId();
		String engineName = value.getEngineName();
		
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
				saveFrame = fObj.frame.save(folderDir.getAbsolutePath());
			} catch(Exception e) {
				e.printStackTrace();
				continue;
			}
			out.beginObject();
			out.name("file").value(saveFrame.getFrameCacheLocation());
			out.name("meta").value(saveFrame.getFrameMetaCacheLocation());
			out.name("state").value(saveFrame.getFrameStateCacheLocation());
			out.name("type").value(saveFrame.getFrameType());
			out.name("name").value(saveFrame.getFrameName());
			out.name("keys");
			out.beginArray();
			for(int i = 0; i < fObj.alias.size(); i++) {
				out.value(fObj.alias.get(i));
			}
			out.endArray();
			out.endObject();
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
	
	
	
	@Override
	public Insight read(JsonReader in) throws IOException {
		Insight insight = new Insight();
		
		in.beginObject();
		in.nextName();
		// engine id, engine name, rdbms id
		if(in.peek() == JsonToken.NULL) {
			in.nextNull();
		} else {
			String engineId = in.nextString();
			insight.setEngineId(engineId);
		}
		
		in.nextName();
		if(in.peek() == JsonToken.NULL) {
			in.nextNull();
		} else {
			String engineName = in.nextString();
			insight.setEngineName(engineName);
		}
		
		in.nextName();
		if(in.peek() == JsonToken.NULL) {
			in.nextNull();
		} else {
			String rdbmsId = in.nextString();
			insight.setRdbmsId(rdbmsId);
		}

		// this will be the varstore
		in.nextName();
		VarStoreAdapter varStoreAdapter = new VarStoreAdapter();
		VarStore store = varStoreAdapter.read(in);
		if(store != null) {
			insight.setVarStore(store);
		}
		// after we potentially set the var store
		// transfer over the default variables
		if(this.existingInsight != null) {
			InsightUtility.transferDefaultVars(this.existingInsight, insight);
		}
		
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
					String path = in.nextString();
					cf.setFrameCacheLocation(path);
				} else if(k.equals("meta")) {
					String path = in.nextString();
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
						String path = in.nextString();
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
				else if(RserveUtil.IS_USER_RSERVE && frame instanceof RDataTable) {
					frame = new RDataTable(insight.getUser());
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

	public void setUserContext(Insight existingInsight) {
		this.existingInsight = existingInsight;		
	}
	
}