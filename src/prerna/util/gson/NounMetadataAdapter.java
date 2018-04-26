package prerna.util.gson;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class NounMetadataAdapter extends TypeAdapter<NounMetadata> {

	private static final Gson GSON = GsonUtility.getDefaultGson();

	@Override
	public NounMetadata read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		String typeStr = in.nextString();
		String className = in.nextString();
		String objStr = in.nextString();
		
		// should start with the type
		PixelDataType type = PixelDataType.valueOf(typeStr);
		Class c = null;
		try {
			c = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Object obj = GSON.fromJson(objStr, c);
		
		List<PixelOperationType> ops = new Vector<PixelOperationType>();
		in.beginArray();
		while(in.hasNext()) {
			ops.add(PixelOperationType.valueOf(in.nextString()));
		}
		in.endArray();

		return new NounMetadata(obj, type, ops);
	}
	
	@Override
	public void write(JsonWriter out, NounMetadata value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		PixelDataType type = value.getNounType();
		Object obj = value.getValue();
		
		out.value(type.toString());
		out.value(obj.getClass().getName());
		out.value(GSON.toJson(value.getValue()));
		out.beginArray();
		List<PixelOperationType> ops = value.getOpType();
		for(PixelOperationType t : ops) {
			out.value(t.toString());
		}
		out.endArray();
	}

}
