/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.IReactor;

public class PackageUtility {

  private static final Logger classLogger = LogManager.getLogger(PackageUtility.class);

  private static final char DOT = '.';
  private static final char SLASH = '/';
  private static final String CLASS_SUFFIX = ".class";
  private static final String BAD_PACKAGE_ERROR =
      "Unable to get resources from path '%s'. Are you sure the given '%s' package exists?";

  public static List<Class<?>> getReactors(String javaPackage) {
    List<Class<?>> classes = PackageUtility.find(javaPackage);
    for (int i = 0; i < classes.size(); i++) {
      Class<?> aClass = classes.get(i);
      boolean validReactor = false;
      // System.out.println(aClass.getSuperclass());
      // //Create an object of the class type
      try {
        Constructor constructor = aClass.getConstructor();
        constructor.newInstance();
        // System.out.println(aClass.getName());
        if (IReactor.class.isAssignableFrom(aClass)) {
          validReactor = true;
        }
      } catch (InstantiationException e) {
        // classLogger.error(Constants.STACKTRACE, e);
      } catch (IllegalAccessException e) {
        // TODO Auto-generated catch block
        classLogger.error(Constants.STACKTRACE, e);
      } catch (IllegalArgumentException e) {
        // TODO Auto-generated catch block
        classLogger.error(Constants.STACKTRACE, e);
      } catch (InvocationTargetException e) {
        // TODO Auto-generated catch block
        classLogger.error(Constants.STACKTRACE, e);
      } catch (NoSuchMethodException e) {
        // TODO Auto-generated catch block
        classLogger.error(Constants.STACKTRACE, e);
      } catch (SecurityException e) {
        // TODO Auto-generated catch block
        classLogger.error(Constants.STACKTRACE, e);
      }
      if (!validReactor) {
        classes.remove(i);
        System.out.println("This class does not implement IReactor " + aClass.getName());
      }
    }
    return classes;
  }

  private static final List<Class<?>> find(final String scannedPackage) {
    final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    final String scannedPath = scannedPackage.replace(DOT, SLASH);
    final Enumeration<URL> resources;
    try {
      resources = classLoader.getResources(scannedPath);
    } catch (IOException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException(
          String.format(BAD_PACKAGE_ERROR, scannedPath, scannedPackage));
    }
    final List<Class<?>> classes = new LinkedList<Class<?>>();
    while (resources.hasMoreElements()) {
      final File file = new File(resources.nextElement().getFile());
      classes.addAll(find(file, scannedPackage));
    }
    return classes;
  }

  private static final List<Class<?>> find(final File file, final String scannedPackage) {
    final List<Class<?>> classes = new LinkedList<Class<?>>();
    if (file.isDirectory()) {
      for (File nestedFile : file.listFiles()) {
        classes.addAll(find(nestedFile, scannedPackage));
      }
      // File names with the $1, $2 holds the anonymous inner classes, we
      // are not interested on them.
    } else if (file.getName().endsWith(CLASS_SUFFIX) && !file.getName().contains("$")) {

      final int beginIndex = 0;
      final int endIndex = file.getName().length() - CLASS_SUFFIX.length();
      final String className = file.getName().substring(beginIndex, endIndex);
      try {
        final String resource = scannedPackage + DOT + className;
        classes.add(Class.forName(resource));
      } catch (ClassNotFoundException ignore) {
      }
    }
    return classes;
  }
}
