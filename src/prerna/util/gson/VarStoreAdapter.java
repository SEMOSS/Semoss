package prerna.util.gson;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;

public class VarStoreAdapter extends TypeAdapter<VarStore> {

	@Override
	public void write(JsonWriter out, VarStore value) throws IOException {
		out.beginObject();
		
		Set<String> keys = value.getKeys();
		
		Map<String, ITask> taskMap = new HashMap<String, ITask>();
		Map<String, ITableDataFrame> frameMap = new HashMap<String, ITableDataFrame>();
		
		// we will go through all the normal keys
		// and ignore the frames / tasks for the time being
		for(String k : keys) {
			NounMetadata noun = value.get(k);
			if(noun.getNounType() == PixelDataType.TASK) {
				taskMap.put(k, (ITask) noun.getValue());
			} else if(noun.getNounType() == PixelDataType.FRAME) {
				frameMap.put(k, (ITableDataFrame) noun.getValue());
			} else {
				// normal noun
				out.name(k);
				NounMetadataAdapter adapter = new NounMetadataAdapter();
				adapter.write(out, noun);
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
}
