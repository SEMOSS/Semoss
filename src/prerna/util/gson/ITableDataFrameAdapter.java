package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.reactor.frame.FrameFactory;

public class ITableDataFrameAdapter extends TypeAdapter<ITableDataFrame> {
	
	@Override
	public void write(JsonWriter out, ITableDataFrame value) throws IOException {
		out.beginObject();
		out.name("type");
		out.value(FrameFactory.getFrameType(value));
		String name = value.getName();
		if(name != null) {
			out.name("name");
			out.value(name);
		}
		out.endObject();
	}

	// 
	@Override
	public ITableDataFrame read(JsonReader in) throws IOException {
		return null;
	}

}
