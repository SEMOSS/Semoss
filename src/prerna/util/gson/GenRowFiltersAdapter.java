package prerna.util.gson;
//package prerna.query.querystruct.adapters;
//
//import java.io.IOException;
//import java.util.List;
//
//import com.google.gson.TypeAdapter;
//import com.google.gson.stream.JsonReader;
//import com.google.gson.stream.JsonWriter;
//
//import prerna.query.querystruct.filters.GenRowFilters;
//import prerna.query.querystruct.filters.IQueryFilter;
//import prerna.query.querystruct.selectors.IQuerySelector;
//
//public class GenRowFiltersAdapter extends TypeAdapter<GenRowFilters> {
//
//	@Override
//	public GenRowFilters read(JsonReader in) throws IOException {
//		// TODO Auto-generated method stub
//		return null;
//	}
//	
//	@Override
//	public void write(JsonWriter out, GenRowFilters value) throws IOException {
//		if (value == null) {
//			out.nullValue();
//			return;
//		}
//
//		out.beginArray();
//		List<IQueryFilter> filters = value.getFilters();
//		int size = filters.size();
//		for(int i = 0; i < size; i++) {
//			
//			
//			
//		}
//
//		
//		TypeAdapter reader = IQuerySelector.getAdapterForSelector(selectorType);
//		reader.write(out, value);
//		
//	}
//
//}
