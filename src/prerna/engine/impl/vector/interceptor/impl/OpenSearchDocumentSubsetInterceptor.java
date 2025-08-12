package prerna.engine.impl.vector.interceptor.impl;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.sf.cglib.proxy.MethodProxy;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.interceptor.AbstractDocumentSubsetInterceptor;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;

public class OpenSearchDocumentSubsetInterceptor extends AbstractDocumentSubsetInterceptor {
	
	public static final Set<String> INTERCEPTED_METHOD_NAMES = Sets.newHashSet(
//			"getNearestNeighborSearchJson"
//			, "getListDocumentSearchJson"
//			, "getListAllRecordsSearchJson"
			"listAllRecords"
			, "listDocuments"
			, "nearestNeighborCall"
			// TODO: , "addDocument", (check edit permission and actually add in target? or just verify in target vector to alter doc list prop)
			// TODO: , "removeDocument" (check edit permission and actually remove in target? or just alter doc list prop)
	);
	
	public static final Set<String> PASSTHROUGH_METHOD_NAMES = Sets.newHashSet(
			"getIndexFilesPath",
			"getDocumentsFilesPath",
			"getFilterAggregation"
	);
	
	public OpenSearchDocumentSubsetInterceptor(IVectorDatabaseEngine target, Object[] constructorArgs) {
		super(target, constructorArgs);
	}
	
	@Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        String methodName = method.getName();
		
		Object result;
		if(INTERCEPTED_METHOD_NAMES.contains(methodName)) {
			System.out.println("Intercepting " + methodName);
        	result = doIntercept(obj, method, args, proxy);
		} else if(PASSTHROUGH_METHOD_NAMES.contains(methodName)) {
        	result = proxy.invoke(target, args);
        } else {
        	System.out.println("Unmapped method called: " + methodName);
        	result = proxy.invoke(target, args);
//        	throw new NotImplementedException("Proxy vector database operation not yet implemented: " + methodName);
        }
        return result;
    }
	
	@SuppressWarnings("unchecked")
	public Object doIntercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		if(!documents.isEmpty()) {
			Map<String, Object> parameters = (Map<String, Object>) args[args.length-1];
			IQueryFilter documentFilter = SimpleQueryFilter.makeColToValFilter("Source", "==", documents);
			List<IQueryFilter> filters = (List<IQueryFilter>) parameters.remove("filters");
			if(filters == null) {
				parameters.put("filters", Lists.newArrayList(documentFilter));
			} else {
				filters.add(documentFilter);
				parameters.put("filters", Lists.newArrayList(new AndQueryFilter(filters)));
			}
		}
		return proxy.invoke(target, args);
	}
	
}
