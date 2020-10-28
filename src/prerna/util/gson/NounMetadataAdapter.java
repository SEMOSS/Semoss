package prerna.util.gson;

import java.io.IOException;
import java.util.Collection;
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
		
		boolean isNull = false;
		boolean isArray = false;
		
		// components of noun meta
		List<String> classNames = new Vector<>();
		List<Object> objList = new Vector<>();
		
		// data type and op types
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
						isNull = true;
					} else if(in.peek() == JsonToken.BEGIN_ARRAY) {
						isArray = true;
						in.beginArray();
						while(in.hasNext()) {
							if(in.peek() == JsonToken.NULL) {
								in.nextNull();
								classNames.add(null);
							} else {
								String className = in.nextString();
								classNames.add(className);
							}
						}
						in.endArray();
					} else {
						String className = in.nextString();
						classNames.add(className);
					}
				} else if(name.equals("value")) {
					in.beginArray();
					int counter = 0;
					while(in.hasNext()) {
						if(in.peek() == JsonToken.NULL) {
							in.nextNull();
							objList.add(null);
						} else {
							String className = classNames.get(counter);
							Class c = null;
							// get the class
							try {
								c = Class.forName(className);
								TypeAdapter adapter = GSON.getAdapter(c);
								objList.add(adapter.read(in));
							} catch (ClassNotFoundException e) {
								e.printStackTrace();
							}
						}
						// increase the index
						counter++;
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
		
		if(isNull) {
			return new NounMetadata(null, type, ops);
		} else if(isArray) {
			return new NounMetadata(objList, type, ops);
		} else {
			return new NounMetadata(objList.get(0), type, ops);
		}
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
			out.name("value");
			out.beginArray();
			out.nullValue();
			out.endArray();
		} else if(obj instanceof Collection) {
			// first grab all the values
			Collection<Object> collectionObj = (Collection<Object>) obj;
			out.name("class");
			out.beginArray();
			for(Object o : collectionObj) {
				if(o == null) {
					out.nullValue();
				} else {
					out.value(o.getClass().getName());
				}
			}
			out.endArray();
			out.name("value");
			out.beginArray();
			for(Object o : collectionObj) {
				if(o == null) {
					out.nullValue();
				} else if(o instanceof ITableDataFrame) {
					writeFrame((ITableDataFrame) obj, out);
				} else {
					TypeAdapter adapter = GSON.getAdapter(o.getClass());
					adapter.write(out, o);
				}
			}
			out.endArray();
			
		} else {
			out.name("class").value(obj.getClass().getName());
			out.name("value");
			out.beginArray();
			// do not break on frames
			if(obj instanceof ITableDataFrame) {
				writeFrame((ITableDataFrame) obj, out);
			} else {
				TypeAdapter adapter = GSON.getAdapter(obj.getClass());
				adapter.write(out, obj);
			}
			out.endArray();
		}
		
		// write the op types
		out.name("opType");
		out.beginArray();
		List<PixelOperationType> opTypes = value.getOpType();
		for(PixelOperationType opType : opTypes) {
			out.value(opType.toString());
		}
		out.endArray();
		
		// end the object
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
