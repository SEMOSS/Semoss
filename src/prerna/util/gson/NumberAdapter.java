package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

public class NumberAdapter extends TypeAdapter<Double> {
	
	/**
	 * Generation of new NumberAdaptor to not send NaN/Infinity to the FE
	 * since they are invalid JSON values
	 */

	@Override 
	public Double read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		return in.nextDouble();
	}

	@Override 
	public void write(JsonWriter out, Double value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		double doubleValue = value.doubleValue();
		if(Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
			out.nullValue();
		} else {
			out.value(value);
		}
	}
}
