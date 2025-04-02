package prerna.engine.impl.rdf;

import java.lang.reflect.InvocationTargetException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ISesameRdfEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public final class RDFDefaultDatabaseTypeFactory {

	private static final Logger classLogger = LogManager.getLogger(RDFDefaultDatabaseTypeFactory.class);

	public static final String DEFAULT_RDF_ENGINE = "DEFAULT_RDF_ENGINE";
	
	private RDFDefaultDatabaseTypeFactory() {
		
	}
	
	public static ISesameRdfEngine getDefaultSesameEngine() {
		ISesameRdfEngine engine = null;
		
		String className = Utility.getDIHelperProperty(DEFAULT_RDF_ENGINE);
		if(className != null && !(className=className.trim()).isEmpty()) {
			try {
				engine = (ISesameRdfEngine) Class.forName(className).getConstructor(null).newInstance();
			} catch (ClassNotFoundException cnfe) {
				classLogger.error(Constants.STACKTRACE, cnfe);
				classLogger.fatal("No such class: " + Utility.cleanLogString(className));
			} catch (InstantiationException ie) {
				classLogger.error(Constants.STACKTRACE, ie);
				classLogger.fatal("Failed instantiation: " + Utility.cleanLogString(className));
			} catch (IllegalAccessException iae) {
				classLogger.error(Constants.STACKTRACE, iae);
				classLogger.fatal("Illegal Access: " + Utility.cleanLogString(className));
			} catch (IllegalArgumentException iare) {
				classLogger.error(Constants.STACKTRACE, iare);
				classLogger.fatal("Illegal argument: " + Utility.cleanLogString(className));
			} catch (InvocationTargetException ite) {
				classLogger.error(Constants.STACKTRACE, ite);
				classLogger.fatal("Invocation exception: " + Utility.cleanLogString(className));
			} catch (NoSuchMethodException nsme) {
				classLogger.error(Constants.STACKTRACE, nsme);
				classLogger.fatal("No constructor: " + Utility.cleanLogString(className));
			} catch (SecurityException se) {
				classLogger.error(Constants.STACKTRACE, se);
				classLogger.fatal("Security exception: " + Utility.cleanLogString(className));
			}
		}
		
		if(engine == null) {
			engine = new RDFFileSesameEngine();
		}
		
		return engine;
	}
	
}
