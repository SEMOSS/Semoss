package prerna.util.gson;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.job.JobReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class VarStoreAdapter extends TypeAdapter<VarStore> {
	
	private Set<String> keysToIgnore = new HashSet<String>();
	
	private boolean collectFrames = false;
	private List<FrameCacheHelper> frames = null;
	
	@Override
	public void write(JsonWriter out, VarStore value) throws IOException {
		out.beginObject();
		
		Set<String> keys = value.getKeys();
		
		// we will go through all the normal keys
		// and ignore the frames / tasks for the time being
		
		if(collectFrames) {
			for(String k : keys) {
				// ignore these 3 variables
				if(k.equals(JobReactor.JOB_KEY) || k.equals(JobReactor.SESSION_KEY) || k.equals(JobReactor.INSIGHT_KEY)) {
					continue;
				}
				NounMetadata noun = value.get(k);
				if(noun.getNounType() == PixelDataType.TASK) {
					continue;
				} else if(noun.getNounType() == PixelDataType.FRAME) {
					ITableDataFrame frame = (ITableDataFrame) noun.getValue();
					FrameCacheHelper existingFrameObject = InsightUtility.findSameFrame(this.frames, frame);
					if(existingFrameObject != null) {
						existingFrameObject.addAlias(k);
					} else {
						FrameCacheHelper fObj = new FrameCacheHelper(frame);
						fObj.addAlias(k);
						this.frames.add(fObj);
					}
				} else {
					if(this.keysToIgnore.contains(k)) {
						continue;
					}
					// normal noun
					out.name(k);
					NounMetadataAdapter adapter = new NounMetadataAdapter();
					adapter.write(out, noun);
				}
			}
			
			// loop through the frames
			// if they contain the keys to ignore
			// remove them
			Iterator<FrameCacheHelper> iterator = this.frames.iterator();
			while(iterator.hasNext()) {
				FrameCacheHelper fObj = iterator.next();
				List<String> alias = fObj.getAlias();
				for(String a : alias) {
					if(this.keysToIgnore.contains(a)) {
						iterator.remove();
					}
				}
			}
		} else {
			for(String k : keys) {
				// ignore these 3 variables
				if(k.equals(JobReactor.JOB_KEY) || k.equals(JobReactor.SESSION_KEY) || k.equals(JobReactor.INSIGHT_KEY)) {
					continue;
				}
				if(this.keysToIgnore.contains(k)) {
					continue;
				}
				// ignore anything that is a task or a frame
				NounMetadata noun = value.get(k);
				if(noun.getNounType() != PixelDataType.TASK && noun.getNounType() != PixelDataType.FRAME) {
					// normal noun
					out.name(k);
					NounMetadataAdapter adapter = new NounMetadataAdapter();
					adapter.write(out, noun);
				}
			}
		}
		
		out.endObject();
	}

	@Override
	public VarStore read(JsonReader in) throws IOException {
		VarStore store = new VarStore();
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			NounMetadataAdapter adapter = new NounMetadataAdapter();
			NounMetadata noun = adapter.read(in);
			store.put(key, noun);
		}
		in.endObject();
		
		return store;
	}
	
	public void setKeysToIgnore(Set<String> keysToIgnore) {
		this.keysToIgnore = keysToIgnore;
	}
	
	public void setCollectFrames(boolean collectFrames) {
		this.collectFrames = collectFrames;
		this.frames = new Vector<FrameCacheHelper>();
	}
	
	public List<FrameCacheHelper> getFrames() {
		return this.frames;
	}
}
