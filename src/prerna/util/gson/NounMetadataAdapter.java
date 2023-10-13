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
import prerna.reactor.IReactor;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class NounMetadataAdapter extends AbstractSemossTypeAdapter<NounMetadata> {

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
		} else if(type == PixelDataType.LAMBDA) {
			Map<String, Object> reactorMap = (Map<String, Object>) objList.get(0);
			String className = (String) reactorMap.get("reactorType");
			Map<String, List<Map<String, Object>>> nounStoreMap = (Map<String, List<Map<String, Object>>>) reactorMap.get("value");
			
			try {
				IReactor thisClass = (IReactor) Class.forName(className).newInstance();
				thisClass.setNounStore(NounStore.generateNounFromMap(nounStoreMap));
				
				return new NounMetadata(thisClass, PixelDataType.LAMBDA);
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new IllegalArgumentException("Unable to create lambda " + className);
			}
		} if(isArray) {
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
			
		} else if(obj instanceof IReactor){
			// you are probably doing something wrong here
			// but i do not want to break serliaizing an object that we 
			// are unable to dos
			Map<String, Object> lambdaMap = new HashMap<>();
	    	lambdaMap.put("reactorType", obj.getClass().getName());
	    	if(((IReactor) obj).getNounStore() != null) {
	    		lambdaMap.put("value", ((IReactor) obj).getStoreMap() );
	    	} else {
	    		lambdaMap.put("value", new HashMap<>());
	    	}
			out.name("class").value(lambdaMap.getClass().getName());
	    	out.name("value");
			out.beginArray();
			TypeAdapter adapter = GSON.getAdapter(lambdaMap.getClass());
			adapter.write(out, lambdaMap);
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
		mapValue.put("type", frame.getFrameType().getTypeAsString());
		String name = frame.getOriginalName();
		if(name != null) {
			mapValue.put("name", name);
			if(!name.equals(frame.getName())) {
				mapValue.put("queryName", frame.getName());
			}
		}
		
		TypeAdapter adapter = GSON.getAdapter(mapValue.getClass());
		adapter.write(out, mapValue);
	}
	
//	public static void main(String[] args) {
//		
//		DateManipulationReactor reactor = new DateManipulationReactor();
//		reactor.In();
//		GenRowStruct grs = new GenRowStruct();
//        grs.add(new NounMetadata("subtraction", PixelDataType.CONST_STRING));
//        reactor.getNounStore().addNoun("type", grs);
//
//		NounMetadata noun = new NounMetadata(reactor, PixelDataType.LAMBDA);
//		Gson gson = GsonUtility.getDefaultGson();
//		String jsonString = gson.toJson(noun);
//		gson.fromJson(jsonString, NounMetadata.class);
//	}

}
