/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.rdf;

import java.lang.reflect.InvocationTargetException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IRDFDatabase;
import prerna.util.Constants;
import prerna.util.Utility;

public final class RDFDefaultDatabaseTypeFactory {

  private static final Logger classLogger =
      LogManager.getLogger(RDFDefaultDatabaseTypeFactory.class);

  public static final String DEFAULT_RDF_ENGINE = "DEFAULT_RDF_ENGINE";

  private RDFDefaultDatabaseTypeFactory() {}

  public static IRDFDatabase getDefaultRdfEngine() {
    IRDFDatabase engine = null;

    String className = Utility.getDIHelperProperty(DEFAULT_RDF_ENGINE);
    if (className != null && !(className = className.trim()).isEmpty()) {
      try {
        engine = (IRDFDatabase) Class.forName(className).getConstructor(null).newInstance();
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

    if (engine == null) {
      //			engine = new EclipseRDF4JFileEngine();
      engine = new RDFJenaTDBEngine();
    }

    return engine;
  }
}
