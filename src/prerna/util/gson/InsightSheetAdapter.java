package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.InsightSheet;

public class InsightSheetAdapter extends TypeAdapter<InsightSheet> {

	@Override
	public void write(JsonWriter out, InsightSheet value) throws IOException {
		// this is just a simple key/value
		out.beginObject();
		out.name("sheetId").value(value.getSheetId());
		out.name("sheetLabel").value(value.getSheetLabel());
		out.name("sheetBackground").value(value.getSheetBackground());
		out.endObject();
	}

	@Override
	public InsightSheet read(JsonReader in) throws IOException {
		String sheetId = null;
		String sheetLabel = null;
		String sheetBackground = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			
			if(key.equals("sheetId")) {
				sheetId = in.nextString();
			} else if(key.equals("sheetLabel")) {
				sheetLabel = in.nextString();
			} else if(key.equals("sheetBackground")) {
				sheetBackground = in.nextString();
			}
		}
		in.endObject();

		InsightSheet sheet = new InsightSheet(sheetId);
		sheet.setSheetLabel(sheetLabel);
		sheet.setSheetBackground(sheetBackground);
		return sheet;
	}

}
