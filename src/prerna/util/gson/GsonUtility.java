package prerna.util.gson;

import java.lang.reflect.Modifier;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.ColorByValueRule;
import prerna.om.HeadersDataRow;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.TemporalEngineHardQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class GsonUtility {

	private static boolean testing = false;
	
	private GsonUtility() {
		
	}
	
	public static Gson getDefaultGson(boolean pretty) {
		GsonBuilder gsonBuilder = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC)
				.registerTypeAdapter(Double.class, new NumberAdapter())
				.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
				
				// qs
				.registerTypeAdapter(TemporalEngineHardQueryStruct.class, new TemporalEngineHardSelectQueryStructAdapter())
				.registerTypeAdapter(HardSelectQueryStruct.class, new HardSelectQueryStructAdapter())
				.registerTypeAdapter(SelectQueryStruct.class, new SelectQueryStructAdapter())
				.registerTypeAdapter(CsvQueryStruct.class, new CsvQueryStructAdapter())
				.registerTypeAdapter(ExcelQueryStruct.class, new ExcelQueryStructAdapter())
				.registerTypeAdapter(UpdateQueryStruct.class, new UpdateQueryStructAdapter())
				.registerTypeAdapter(ColorByValueRule.class, new ColorByValueRuleAdapter())
				
				// selectors
				.registerTypeAdapter(IQuerySelector.class, new IQuerySelectorAdapter())
				.registerTypeAdapter(QueryColumnSelector.class, new QueryColumnSelectorAdapter())
				.registerTypeAdapter(QueryFunctionSelector.class, new QueryFunctionSelectorAdapter())
				.registerTypeAdapter(QueryArithmeticSelector.class, new QueryArithmeticSelectorAdapter())
				.registerTypeAdapter(QueryConstantSelector.class, new QueryConstantSelectorAdapter())
				.registerTypeAdapter(QueryOpaqueSelector.class, new QueryOpaqueSelectorAdapter())
				.registerTypeAdapter(QueryIfSelector.class, new QueryIfSelectorAdapter())
				
				// filters
				.registerTypeAdapter(GenRowFilters.class, new GenRowFiltersAdapter())
				.registerTypeAdapter(IQueryFilter.class, new IQueryFilterAdapter())
				.registerTypeAdapter(SimpleQueryFilter.class, new SimpleQueryFilterAdapter())
				.registerTypeAdapter(OrQueryFilter.class, new OrQueryFilterAdapter())
				.registerTypeAdapter(AndQueryFilter.class, new AndQueryFilterAdapter())
				.registerTypeAdapter(FunctionQueryFilter.class, new FunctionQueryFilterAdapter())
				
				// sorts
				.registerTypeAdapter(IQuerySort.class, new IQuerySortAdapter())
				.registerTypeAdapter(QueryColumnOrderBySelector.class, new QueryColumnOrderBySelectorAdapter())
				.registerTypeAdapter(QueryCustomOrderBy.class, new QueryCustomOrderByAdapter())

				// noun meta
				.registerTypeAdapter(NounMetadata.class, new NounMetadataAdapter())
				.registerTypeAdapter(VarStore.class, new VarStoreAdapter())

				// iterators
				.registerTypeAdapter(BasicIteratorTask.class, new BasicIteratorTaskAdapter())
				
				// OLD LEGACY STUFF
				.registerTypeAdapter(SEMOSSVertex.class, new SEMOSSVertexAdapter())
				.registerTypeAdapter(SEMOSSEdge.class, new SEMOSSEdgeAdapter())
				
				// cluster
				.registerTypeAdapter(IHeadersDataRow.class, new IHeadersDataRowAdapter())
				.registerTypeAdapter(HeadersDataRow.class, new HeadersDataRowAdapter())
				
				// pixel objects
				.registerTypeAdapter(Pixel.class, new PixelAdapter())
				.registerTypeAdapter(PixelList.class, new PixelListAdapter())
				;
		
		if(pretty) {
			gsonBuilder.setPrettyPrinting();
		}
		
		return gsonBuilder.create();
	}
	
	public static Gson getDefaultGson() {
		return getDefaultGson(testing);
	}
	
}
