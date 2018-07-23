package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.algorithm.api.ITableDataFrame;

public class ITableDataFrameAdapter extends TypeAdapter<ITableDataFrame> {
	
	@Override
	public void write(JsonWriter out, ITableDataFrame value) throws IOException {
		out.beginObject();
		
		
		
	}

	@Override
	public ITableDataFrame read(JsonReader in) throws IOException {
		return null;
	}

}
