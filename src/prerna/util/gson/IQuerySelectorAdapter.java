package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;

public class IQuerySelectorAdapter extends TypeAdapter<IQuerySelector> {

	@Override
	public IQuerySelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// should start with the type
		String selectorTypeString = in.nextString();
		IQuerySelector.SELECTOR_TYPE selectorType = IQuerySelector.convertStringToSelectorType(selectorTypeString);
		
		TypeAdapter reader = IQuerySelector.getAdapterForSelector(selectorType);
		return (IQuerySelector) reader.read(in);
	}
	
	@Override
	public void write(JsonWriter out, IQuerySelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// go to the specific instance impl to write it
		IQuerySelector.SELECTOR_TYPE selectorType = value.getSelectorType();
		TypeAdapter reader = IQuerySelector.getAdapterForSelector(selectorType);
		reader.write(out, value);
	}

}
