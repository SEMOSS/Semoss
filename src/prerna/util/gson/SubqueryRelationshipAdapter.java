package prerna.util.gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;

public class SubqueryRelationshipAdapter extends AbstractSemossTypeAdapter<SubqueryRelationship> implements IRelationAdapterHelper {

	@Override
	public SubqueryRelationship read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// remove the beginning objects
		in.beginObject();
		in.nextName();
		in.nextString();
		in.nextName();
		
		// now we read the actual content
		SubqueryRelationship value = readContent(in);
		in.endObject();
		return value;
	}
	
	@Override
	public SubqueryRelationship readContent(JsonReader in) throws IOException {
		String queryAlias = null;
		String joinType = null;
		SelectQueryStruct qs = null;
		List<String[]> joinOnDetails = new ArrayList<>();
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			if(name.equals("queryAlias")) {
				queryAlias = in.nextString();
			} else if(name.equals("joinType")) {
				joinType = in.nextString();
			} else if(name.equals("qs")) {
				SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
				qs = adapter.read(in);
			} else if(name.equals("joinOnDetails")) {
				in.beginArray();
				while(in.hasNext()) {
					List<String> joinOn = new ArrayList<>();
					in.beginArray();
					while(in.hasNext()) {
						if(in.peek() == null) {
							joinOn.add(null);
							in.nextNull();
						} else {
							joinOn.add(in.nextString());
						}
					}
					in.endArray();
					// store the joinOn
					joinOnDetails.add(joinOn.toArray(new String[] {}));
				}
			}
		}
		in.endObject();
		
		return new SubqueryRelationship(qs, queryAlias, joinType, joinOnDetails);
	}

	@Override
	public void write(JsonWriter out, SubqueryRelationship value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		out.beginObject();
		out.name("type").value(IRelation.RELATION_TYPE.BASIC.toString());
		out.name("content");
			// content object
			out.beginObject();
				out.name("queryAlias").value(value.getQueryAlias());
				out.name("joinType").value(value.getJoinType());
				out.name("qs");
				SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
				adapter.write(out, value.getQs());
				List<String[]> joinOnDetails = value.getJoinOnDetails();
				out.name("joinOnDetails");
				out.beginArray();
				for(int i = 0; i < joinOnDetails.size(); i++) {
					String[] joinOn = joinOnDetails.get(i);
					out.beginArray();
					for(int j = 0; i < joinOn.length; j++) {
						out.value(joinOn[j]);
					}
					out.endArray();
				}
				out.endArray();
			out.endObject();
		out.endObject();
	}

}
