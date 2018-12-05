package prerna.util.gson;

import java.lang.reflect.Modifier;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.date.SemossDate;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GsonUtility {

	private static boolean testing = false;
	
	private GsonUtility() {
		
	}
	
	public static Gson getDefaultGson() {
		GsonBuilder gsonBuilder = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.STATIC)
				.registerTypeAdapter(Double.class, new NumberAdapter())
				.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
				
				// qs
				.registerTypeAdapter(SelectQueryStruct.class, new SelectQueryStructAdapter())
				.registerTypeAdapter(HardSelectQueryStruct.class, new HardSelectQueryStructAdapter())
				
				// selectors
				.registerTypeAdapter(IQuerySelector.class, new IQuerySelectorAdapter())
				.registerTypeAdapter(QueryColumnSelector.class, new QueryColumnSelectorAdapter())
				.registerTypeAdapter(QueryFunctionSelector.class, new QueryFunctionSelectorAdapter())
				.registerTypeAdapter(QueryArithmeticSelector.class, new QueryArithmeticSelectorAdapter())
				.registerTypeAdapter(QueryConstantSelector.class, new QueryConstantSelectorAdapter())
				.registerTypeAdapter(QueryOpaqueSelector.class, new QueryOpaqueSelectorAdapter())
				
				// filters
				.registerTypeAdapter(GenRowFilters.class, new GenRowFiltersAdapter())
				.registerTypeAdapter(IQueryFilter.class, new IQueryFilterAdapter())
				.registerTypeAdapter(SimpleQueryFilter.class, new SimpleQueryFilterAdapter())
				.registerTypeAdapter(OrQueryFilter.class, new OrQueryFilterAdapter())
				.registerTypeAdapter(AndQueryFilter.class, new AndQueryFilterAdapter())

				// noun meta
				.registerTypeAdapter(NounMetadata.class, new NounMetadataAdapter())
				.registerTypeAdapter(VarStore.class, new VarStoreAdapter())
//				.registerTypeAdapter(Insight.class, new InsightAdapter())

				// OLD LEGACY STUFF
				.registerTypeAdapter(SEMOSSVertex.class, new SEMOSSVertexAdapter())
				.registerTypeAdapter(SEMOSSEdge.class, new SEMOSSEdgeAdapter())
				
				// cluster
				.registerTypeAdapter(IHeadersDataRow.class, new IHeadersDataRowAdapter())
				.registerTypeAdapter(HeadersDataRow.class, new HeadersDataRowAdapter())
				;
		
		if(testing) {
			gsonBuilder.setPrettyPrinting();
		}
		
		return gsonBuilder.create();
	}
	
}
