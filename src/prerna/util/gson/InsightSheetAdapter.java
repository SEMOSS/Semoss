package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.InsightSheet;

public class InsightSheetAdapter extends AbstractSemossTypeAdapter<InsightSheet> {

	@Override
	public void write(JsonWriter out, InsightSheet value) throws IOException {
		// this is just a simple key/value
		String sheetId = value.getSheetId();
		String sheetLabel = value.getSheetLabel();
		String backgroundColor = value.getBackgroundColor();
		Boolean hideHeaders = value.getHideHeaders();
		Boolean hideBorders = value.getHideBorders();
		int borderSize = value.getBorderSize();
		
		out.beginObject();
		out.name("sheetId").value(sheetId);
		if(sheetLabel == null) {
			out.name("sheetLabel").nullValue();
		} else {
			out.name("sheetLabel").value(sheetLabel);
		}
		if(backgroundColor == null) {
			out.name("backgroundColor").nullValue();
		} else {
			out.name("backgroundColor").value(backgroundColor);
		}
		if(hideHeaders == null) {
			out.name("hideHeaders").nullValue();
		} else {
			out.name("hideHeaders").value(hideHeaders);
		}
		if(hideBorders == null) {
			out.name("hideBorders").nullValue();
		} else {
			out.name("hideBorders").value(hideBorders);
		}
		
		// border size has default of 2
		out.name("borderSize").value(borderSize);
		
		out.endObject();
	}

	@Override
	public InsightSheet read(JsonReader in) throws IOException {
		String sheetId = null;
		String sheetLabel = null;
		String backgroundColor = null;
		Boolean hideHeaders = null;
		Boolean hideBorders = null;
		Integer borderSize = null;
		
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
			} else if(key.equals("backgroundColor")) {
				backgroundColor = in.nextString();
			} else if(key.equals("hideHeaders")) {
				hideHeaders = in.nextBoolean();
			} else if(key.equals("hideBorders")) {
				hideBorders = in.nextBoolean();
			} else if(key.equals("borderSize")) {
				borderSize = in.nextInt();
			}
		}
		in.endObject();

		InsightSheet sheet = new InsightSheet(sheetId);
		sheet.setSheetLabel(sheetLabel);
		sheet.setBackgroundColor(backgroundColor);
		sheet.setHideHeaders(hideHeaders);
		sheet.setHideBorders(hideBorders);
		if(borderSize != null) {
			sheet.setBorderSize(borderSize);
		}
		return sheet;
	}

}
