package prerna.engine.impl.vector.interceptor;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import prerna.engine.api.IVectorDatabaseEngine;

public abstract class AbstractDocumentSubsetInterceptor extends AbstractInterceptor {
	
	protected final Set<String> documents;
	
	public AbstractDocumentSubsetInterceptor(IVectorDatabaseEngine target, Object[] constructorArgs) {
		super(target, constructorArgs);
		if(constructorArgs != null) {
			if(constructorArgs.length > 0){
				String[] stringArgs = new String[constructorArgs.length];
				for(int i=0; i<constructorArgs.length; i++) {
					stringArgs[i] = constructorArgs[i] == null ? null : constructorArgs[i].toString();
				}
				documents = Sets.newHashSet(stringArgs);
				if(documents.contains("*")) {
					documents.clear();
				}
			} else {
				documents = new HashSet<>();
			}
		} else {
			documents = new HashSet<>();
		}
	}
	
}
