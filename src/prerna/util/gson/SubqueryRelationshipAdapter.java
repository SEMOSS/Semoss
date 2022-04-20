package prerna.util.gson;

import java.io.IOException;

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
		String fromConcept = null;
		String joinType = null;
		String toConcept = null;
		String comparator = null;
		String queryAlias = null;
		SelectQueryStruct qs = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			if(name.equals("fromConcept")) {
				fromConcept = in.nextString();
			} else if(name.equals("joinType")) {
				joinType = in.nextString();
			} else if(name.equals("toConcept")) {
				toConcept = in.nextString();
			} else if(name.equals("comparator")) {
				comparator = in.nextString();
			} else if(name.equals("queryAlias")) {
				queryAlias = in.nextString();
			} else if(name.equals("qs")) {
				SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
				 qs = adapter.read(in);
			}
		}
		in.endObject();
		
		return new SubqueryRelationship(qs, queryAlias, new String[] {fromConcept, joinType, toConcept, comparator});
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
				out.name("fromConcept").value(value.getFromConcept());
				out.name("joinType").value(value.getJoinType());
				out.name("toConcept").value(value.getToConcept());
				out.name("comparator").value(value.getComparator());
				out.name("queryAlias").value(value.getQueryAlias());
				out.name("qs");
				SelectQueryStructAdapter adapter = new SelectQueryStructAdapter();
				adapter.write(out, value.getQs());
			out.endObject();
		out.endObject();
	}

}
