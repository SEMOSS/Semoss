package prerna.engine.impl.vector.interceptor.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import net.sf.cglib.proxy.MethodProxy;
import prerna.engine.api.IEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.interceptor.AbstractDocumentSubsetInterceptor;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.util.EngineUtility;
import prerna.util.Utility;


public class OpenSearchDocumentSubsetInterceptor extends AbstractDocumentSubsetInterceptor {
	
	private static final Logger classLogger = LogManager.getLogger(OpenSearchDocumentSubsetInterceptor.class);
	protected static final String FILE_SEPARATOR = "/";

	public static final Set<String> INTERCEPTED_METHOD_NAMES = Sets.newHashSet(
			"listAllRecords"
			, "listDocuments"
			, "nearestNeighbor"
			, "removeDocument"
			, "addDocument"
			, "getDocumentsFilesPath"
	);
	
	public OpenSearchDocumentSubsetInterceptor(IVectorDatabaseEngine proxyEngine, IVectorDatabaseEngine targetEngine, Object[] constructorArgs) {
		super(proxyEngine, targetEngine, constructorArgs);
	}
	
	@SuppressWarnings("unchecked")
    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxyMethod) throws Throwable {
        String methodName = method.getName();
        Object result;
        if (INTERCEPTED_METHOD_NAMES.contains(methodName)) {
        	// Customized interceptor logic!
            if ("removeDocument".equals(methodName)) {
                // Customized interceptor logic!
                List<String> toRemove = (List<String>) args[0];
                // TODO: support removing from * to get a not in condition
                boolean changed = documents != null && documents.removeAll(toRemove);
                if (changed) {
                    // Also remove from the persistent config file (TARGET_PARAMETERS) for the proxy
                    writeBackDocumentSubset((AbstractEngine) proxyEngine, documents);
                    classLogger.info("[OpenSearch Proxy] Removed from subset: " + toRemove);
                }
                return null;
            } else if ("addDocument".equals(methodName)) {
                List<String> toAdd = (List<String>) args[0];
                boolean changed = false;
                for(String s : toAdd) {
                	changed = documents != null && documents.add(FilenameUtils.getName(s)) || changed;
                }
                if (changed) {
                    // Also add to the persistent config file (TARGET_PARAMETERS) for the proxy
                    writeBackDocumentSubset((AbstractEngine) proxyEngine, documents);
                    classLogger.info("[OpenSearch Proxy] Added to subset: " + toAdd);
                }
                return null;
            } else if ("getDocumentsFilesPath".equals(methodName)) {
            	return getDocumentsFilesPath((AbstractVectorDatabaseEngine) proxyEngine, null);
            }
            
            // Otherwise, standard filter+proxy call logic
            result = doIntercept(obj, method, args, proxyMethod);
        } else {
            result = proxyMethod.invokeSuper(obj, args);
        }
        return result;
    }
	
	@SuppressWarnings("unchecked")
	public Object doIntercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
		// null documents means no filter needed
		// empty documents means no files visible
		if(documents != null) {
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
		return proxy.invokeSuper(obj, args);
	}
	
	
    /**
     * Persist the filtered documents subset to disk, in the proxy's .smss
     * Updates the TARGET_PARAMETERS field, which is a JSON array
     */
    private void writeBackDocumentSubset(AbstractEngine proxyEngine, Set<String> subset) throws IOException {
        // Step 1: Load .smss properties file
        String smssFilePath = proxyEngine.getSmssFilePath();
        File smssFile = new File(smssFilePath);
        if (!smssFile.exists())
            throw new IOException("Cannot find .smss config file for proxy engine at " + smssFilePath);
        Properties props = new Properties();
        try (var is = Files.newInputStream(smssFile.toPath())) {
            props.load(is);
        }

        // Step 2: Convert the current subset to a pretty JSON array string for the field
        Gson gson = new Gson();
        String newVal = gson.toJson(subset);

        // Step 3: Update the TARGET_PARAMETERS property
        props.setProperty("TARGET_PARAMETERS", newVal);

        // Step 4: Write back to file (overwrite)
        try (FileWriter fw = new FileWriter(smssFile, false)) {
            props.store(fw, null);
        }
    }
    
	public String getDocumentsFilesPath(AbstractVectorDatabaseEngine proxyEngine, String indexClass) {
		
		// highest directory (first layer inside vector db base folder)
		String engineDir = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.VECTOR, proxyEngine.getEngineId(), proxyEngine.getEngineName());
		
		// second layer - This holds all the different "tables". The reason we want this is to easily and quickly grab the sub folders
		File schemaFolder = new File(engineDir, "schema");
		
				
		if(indexClass == null || (indexClass=indexClass.trim()).isEmpty()) {
			indexClass = "default";
		}

		return Utility.normalizePath(schemaFolder.getAbsolutePath() + FILE_SEPARATOR + indexClass + FILE_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
	}
	
}
