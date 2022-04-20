package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;

public class BasicRelationshipAdapter extends AbstractSemossTypeAdapter<BasicRelationship> implements IRelationAdapterHelper {

	@Override
	public BasicRelationship read(JsonReader in) throws IOException {
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
		BasicRelationship value = readContent(in);
		in.endObject();
		return value;
	}
	
	@Override
	public BasicRelationship readContent(JsonReader in) throws IOException {
		String fromConcept = null;
		String joinType = null;
		String toConcept = null;
		String comparator = null;
		String relationName = null;
		
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
			} else if(name.equals("relationName")) {
				relationName = in.nextString();
			}
		}
		in.endObject();
		
		return new BasicRelationship(fromConcept, joinType, toConcept, comparator, relationName);
	}

	@Override
	public void write(JsonWriter out, BasicRelationship value) throws IOException {
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
				out.name("relationName").value(value.getRelationName());
			out.endObject();
		out.endObject();
	}

}
