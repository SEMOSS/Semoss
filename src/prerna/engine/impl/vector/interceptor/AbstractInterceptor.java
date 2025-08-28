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
package prerna.engine.impl.vector.interceptor;

import java.lang.reflect.Constructor;
import net.sf.cglib.proxy.MethodInterceptor;
import prerna.engine.api.IVectorDatabaseEngine;

public abstract class AbstractInterceptor implements MethodInterceptor {

  protected final IVectorDatabaseEngine proxyEngine;
  protected final IVectorDatabaseEngine targetEngine;
  protected final Object[] constructorArgs;

  public AbstractInterceptor(
      IVectorDatabaseEngine proxyEngine,
      IVectorDatabaseEngine targetEngine,
      Object[] constructorArgs) {
    if (targetEngine == null) {
      throw new IllegalArgumentException("The vector database target for the proxy is undefined");
    }
    this.proxyEngine = proxyEngine;
    this.targetEngine = targetEngine;
    this.constructorArgs = constructorArgs;
  }

  @SuppressWarnings("unchecked")
  public static MethodInterceptor buildInterceptor(
      String interceptorClassName,
      IVectorDatabaseEngine proxy,
      IVectorDatabaseEngine target,
      Object[] constructorArgs)
      throws Exception {
    Class<? extends MethodInterceptor> interceptorClass;
    try {
      interceptorClass = (Class<? extends MethodInterceptor>) Class.forName(interceptorClassName);
    } catch (ClassNotFoundException | ClassCastException e) {
      e.printStackTrace();
      throw e;
    }
    return buildInterceptor(interceptorClass, proxy, target, constructorArgs);
  }

  @SuppressWarnings("unchecked")
  public static <T extends MethodInterceptor> T buildInterceptor(
      Class<T> interceptorClass,
      IVectorDatabaseEngine proxy,
      IVectorDatabaseEngine target,
      Object[] constructorArgs)
      throws Exception {
    Constructor<?> constructor;
    try {
      constructor =
          interceptorClass.getConstructor(
              IVectorDatabaseEngine.class, IVectorDatabaseEngine.class, Object[].class);
      return (T) constructor.newInstance(proxy, target, constructorArgs);
    } catch (NoSuchMethodException | SecurityException e) {
      e.printStackTrace();
      throw e;
    }
  }
}
