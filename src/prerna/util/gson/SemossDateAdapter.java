package prerna.util.gson;

import java.io.IOException;
import java.util.Date;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.date.SemossDate;
import prerna.util.Utility;

public class SemossDateAdapter extends TypeAdapter<SemossDate> {

	@Override 
	public SemossDate read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		String dateStr = in.nextString();
		Date d = Utility.getDateAsDateObj(dateStr);
		if(d != null) {
			return new SemossDate(d);
		}
		d = Utility.getTimeStampAsDateObj(dateStr);
		if(d != null) {
			return new SemossDate(d, "yyyy-MM-dd hh:mm:ss");
		}
		return null;
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