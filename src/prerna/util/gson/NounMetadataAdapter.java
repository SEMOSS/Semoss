package prerna.util.gson;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.FrameFactory;

public class NounMetadataAdapter extends TypeAdapter<NounMetadata> {

	private static final Gson GSON = GsonUtility.getDefaultGson();

	@Override
	public NounMetadata read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		String className;
		Class c = null;

		// components of noun meta
		Object obj = null;
		PixelDataType type = null;
		List<PixelOperationType> ops = new Vector<PixelOperationType>();

		in.beginObject();
		while(in.hasNext()) {
			if(in.peek() == JsonToken.NAME) {
				String name = in.nextName();
				if(name.equals("pixelType")) {
					String typeStr = in.nextString();
					type = PixelDataType.valueOf(typeStr);
				} else if(name.equals("class")) {
					if(in.peek() == JsonToken.NULL) {
						in.nextNull();
					} else {
						className = in.nextString();
						// get the class
						try {
							c = Class.forName(className);
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					}
				} else if(name.equals("value")) {
					in.beginArray();
					while(in.hasNext()) {
						if(in.peek() == JsonToken.NULL) {
							in.nextNull();
						} else {
							TypeAdapter adapter = GSON.getAdapter(c);
							obj = adapter.read(in);
						}
					}
					in.endArray();
				} else if(name.equals("opType")) {
					in.beginArray();
					while(in.hasNext()) {
						ops.add(PixelOperationType.valueOf(in.nextString()));
					}
					in.endArray();
				}
			}
		}
		in.endObject();
		
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
		
		out.beginObject();
		out.name("pixelType").value(type.toString());
		if(obj == null) {
			out.name("class").nullValue();
		} else {
			out.name("class").value(obj.getClass().getName());
		}
		out.name("value");
		out.beginArray();
		if(obj == null) {
			out.nullValue();
		} else {
			// do not break on frames
			if(obj instanceof ITableDataFrame) {
				writeFrame((ITableDataFrame) obj, out);
			} else {
				TypeAdapter adapter = GSON.getAdapter(obj.getClass());
				adapter.write(out, obj);
			}
		}
		out.endArray();
		
		out.name("opType");
		out.beginArray();
		List<PixelOperationType> opTypes = value.getOpType();
		for(PixelOperationType opType : opTypes) {
			out.value(opType.toString());
		}
		out.endArray();
		out.endObject();
	}
	
	/**
	 * Write the frame as a map
	 * @param frame
	 * @param out
	 * @throws IOException
	 */
	private void writeFrame(ITableDataFrame frame, JsonWriter out) throws IOException {
		Map<String, String> mapValue = new HashMap<String, String>();
		mapValue.put("type", FrameFactory.getFrameType(frame));
		String name = frame.getName();
		if(name != null) {
			mapValue.put("name", name);
		}
		
		TypeAdapter adapter = GSON.getAdapter(mapValue.getClass());
		adapter.write(out, mapValue);
	}
	

}
