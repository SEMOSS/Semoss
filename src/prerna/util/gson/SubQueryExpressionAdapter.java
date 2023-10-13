package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.reactor.qs.SubQueryExpression;
import prerna.query.querystruct.SelectQueryStruct;

public class SubQueryExpressionAdapter extends AbstractSemossTypeAdapter<SubQueryExpression> {
	
	@Override
	public SubQueryExpression read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		SubQueryExpression value = new SubQueryExpression();
		SelectQueryStruct qs = null;
		QUERY_STRUCT_TYPE qsType = null;
		in.beginObject();
		in.nextName();
		if(in.peek() == JsonToken.NULL) {
			// we have an empty object
			in.nextNull();
		} else {
			qsType = QUERY_STRUCT_TYPE.valueOf(in.nextString());
		}
		in.nextName();
		if(in.peek() == JsonToken.NULL) {
			// we have an empty object
			in.nextNull();
		} else {
			AbstractSemossTypeAdapter adapter = AbstractQueryStruct.getAdapterForQueryStruct(qsType);
			adapter.setInsight(this.insight);
			qs = (SelectQueryStruct) adapter.read(in);
		}
		in.endObject();

		value.setQs(qs);
		value.setInsight(this.insight);
		return value;
	}
	
	@Override
	public void write(JsonWriter out, SubQueryExpression value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		out.beginObject();
		// write the QS
		if(value.getQs() == null) {
			out.name("qsType");
			out.nullValue();
			out.name("qs");
			out.nullValue();
		} else {
			SelectQueryStruct qs = value.getQs();
			out.name("qsType");
			out.value(qs.getQsType() + "");
			out.name("qs");
			TypeAdapter adapter = AbstractQueryStruct.getAdapterForQueryStruct(qs.getQsType());
			adapter.write(out, qs);
		}
		out.endObject();
	}
	
}
