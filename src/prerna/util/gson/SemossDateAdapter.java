package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.date.SemossDate;

public class SemossDateAdapter extends TypeAdapter<SemossDate> {

	@Override 
	public SemossDate read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		String dateStr = in.nextString();
		SemossDate date = SemossDate.genDateObj(dateStr);
		if(date == null) {
			date = SemossDate.genTimeStampDateObj(dateStr);
		}
		return date;
	}

	@Override 
	public void write(JsonWriter out, SemossDate value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		out.value(value.toString());
	}
}