package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.joins.IRelation;

public class IRelationAdapter extends AbstractSemossTypeAdapter<IRelation> {

	@Override
	public IRelation read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// should start with the type
		in.beginObject();
		in.nextName();
		String relationType = in.nextString();
		
		// get the correct adapter
		IRelation.RELATION_TYPE relationshipType = IRelation.convertStringToRelationType(relationType);
		TypeAdapter reader = IRelation.getAdapterForRelation(relationshipType);
		if(reader instanceof IRelationAdapterHelper) {
			// now we should have the content object
			in.nextName();
			IRelation relation = ((IRelationAdapterHelper) reader).readContent(in);
			in.endObject();
			
			return relation;
		} else {
			// this is the case of a subquery as a filter
			return (IRelation) reader.read(in);
		}
	}

	@Override
	public void write(JsonWriter out, IRelation value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// go to the specific instance impl to write it
		IRelation.RELATION_TYPE relationType = value.getRelationType();
		TypeAdapter adapter = IRelation.getAdapterForRelation(relationType);
		adapter.write(out, value);
	}

}
