package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;

public class IQuerySelectorAdapter extends AbstractSemossTypeAdapter<IQuerySelector> {

	@Override
	public IQuerySelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// should start with the type
		in.beginObject();
		in.nextName();
		String selectorTypeString = in.nextString();
		
		// get the correct adapter
		IQuerySelector.SELECTOR_TYPE selectorType = IQuerySelector.convertStringToSelectorType(selectorTypeString);
		AbstractSemossTypeAdapter reader = IQuerySelector.getAdapterForSelector(selectorType);
		reader.setInsight(this.insight);
		
		// now we should have the content object
		in.nextName();
		IQuerySelector selector = ((IQuerySelectorAdapterHelper) reader).readContent(in);
		in.endObject();
		
		return selector;
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
