package prerna.util.gson;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.om.SEMOSSVertex;

public class SEMOSSVertexAdapter extends TypeAdapter<SEMOSSVertex> {

	
	@Override
	public void write(JsonWriter out, SEMOSSVertex value) throws IOException {
		String uri = value.getURI();
		Hashtable propHash = value.getProperty();

		out.beginObject();
		out.name("uri").value(uri);
		out.name("propHash");
		out.beginObject();
		// flush out properties
		for(Object key : propHash.keySet()) {
			out.name(key.toString());
			
			Object innerObj = propHash.get(key);
			if(innerObj instanceof Map) {
				out.beginObject();
				Map inner = (Map) propHash.get(key);
				for(Object innerKey : inner.keySet()) {
					out.name(innerKey.toString());
					Object innerValue = inner.get(innerKey);
					if(innerValue instanceof Number){
						out.value((Number) innerValue);
					} else {
						out.value(innerValue.toString());
					}
				}
				out.endObject();
			} else if(innerObj instanceof Number){
				Number num = (Number) innerObj;
				if(Double.isNaN(num.doubleValue())) {
					out.value(0);
				} else {
					out.value((Number) innerObj);
				}
			} else {
				out.value(innerObj.toString());
			}
		}
		out.endObject();
		out.endObject();
	}

	@Override
	public SEMOSSVertex read(JsonReader in) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}


}
