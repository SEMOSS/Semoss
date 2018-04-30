package prerna.util.gson;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.om.SEMOSSEdge;

public class SEMOSSEdgeAdapter extends TypeAdapter<SEMOSSEdge> {

	@Override
	public void write(JsonWriter out, SEMOSSEdge value) throws IOException {
		String uri = value.getURI();
		String source = value.outVertex.getURI();
		String target = value.inVertex.getURI();
		Hashtable propHash = value.getProperty();
		
		out.beginObject();
		out.name("uri").value(uri);
		out.name("source").value(source);
		out.name("target").value(target);
		out.name("propHash");
		out.beginObject();
		// flush out properties
		for(Object key : propHash.keySet()) {
			out.name(key.toString());
			Object innerObj = propHash.get(key);
			writePropHash(out, innerObj);
		}
		out.endObject();
		out.endObject();
	}
	
	private void writePropHash(JsonWriter out, Object obj) throws IOException {
		if(obj instanceof Map) {
			out.beginObject();
			Map mapObj = (Map) obj;
			for(Object key : mapObj.keySet()) {
				out.name(key.toString());
				Object value = mapObj.get(key);
				if(value instanceof Map) {
					// make it recursive
					writePropHash(out, value);
				} else if(value instanceof Number){
					out.value((Number) value);
				} else {
					out.value(value.toString());
				}
			}
			out.endObject();
		} else if(obj instanceof Number){
			Number num = (Number) obj;
			if(Double.isNaN(num.doubleValue())) {
				out.value(0);
			} else {
				out.value((Number) obj);
			}
		} else {
			out.value(obj.toString());
		}
	}

	@Override
	public SEMOSSEdge read(JsonReader in) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
