package prerna.engine.impl.vector.interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;


import prerna.engine.api.IVectorDatabaseEngine;

public abstract class AbstractDocumentSubsetInterceptor extends AbstractInterceptor {
	
	protected final Set<String> documents;
	
	public AbstractDocumentSubsetInterceptor(IVectorDatabaseEngine proxyEngine, IVectorDatabaseEngine targetEngine, Object[] constructorArgs) {
		super(proxyEngine, targetEngine, constructorArgs);
		// null documents means no filter needed.
		// empty documents filter means no files allowed.
		if(constructorArgs != null) {
			if(constructorArgs.length > 0){
				String[] stringArgs = new String[constructorArgs.length];
				for(int i=0; i<constructorArgs.length; i++) {
					stringArgs[i] = constructorArgs[i] == null ? null : constructorArgs[i].toString();
				}
				Set<String> documentsGiven = new TreeSet<>(UTF8_BYTE_ORDER_COMPARATOR);
				Collections.addAll(documentsGiven, stringArgs);
				if(documentsGiven.contains("*")) {
					documents = null;
				} else {
					documents = documentsGiven;
				}
			} else {
				documents = new TreeSet<>(UTF8_BYTE_ORDER_COMPARATOR);
			}
		} else {
			documents = null;
		}
	}
	
	private static final Comparator<String> UTF8_BYTE_ORDER_COMPARATOR = (s1, s2) -> {
	    if (s1 == null && s2 == null) return 0;
	    if (s1 == null) return -1;
	    if (s2 == null) return 1;
	    byte[] b1 = s1.getBytes(StandardCharsets.UTF_8);
	    byte[] b2 = s2.getBytes(StandardCharsets.UTF_8);
	    int len = Math.min(b1.length, b2.length);
	    for (int i = 0; i < len; i++) {
	        int cmp = Byte.compare(b1[i], b2[i]);
	        if (cmp != 0) return cmp;
	    }
	    return Integer.compare(b1.length, b2.length);
	};
	
}
