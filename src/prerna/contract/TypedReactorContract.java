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
package prerna.contract;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

/** Inspects a concrete record-based execution signature without initializing its class. */
public record TypedReactorContract(Class<? extends Record> input, Method execution) {
    public static TypedReactorContract inspect(Class<?> reactor) {
        // Require a concrete declaration, rather than accidentally inheriting another operation's identity.
        List<Method> methods = Arrays.stream(reactor.getDeclaredMethods())
                .filter(method -> method.getName().equals("execute") && !method.isBridge() && !method.isSynthetic()
                        && method.getParameterCount() == 1 && method.getParameterTypes()[0].isRecord()).toList();
        if (methods.size() != 1) throw new IllegalArgumentException(reactor.getName()
                + " must declare exactly one execute(InputRecord) method");
        Method method = methods.getFirst();
        if (Modifier.isStatic(method.getModifiers()) || Modifier.isAbstract(method.getModifiers()))
            throw new IllegalArgumentException("Typed execute must be a concrete instance method");
        if (!method.isAnnotationPresent(Output.class))
            throw new IllegalArgumentException("Typed execute must declare @Output(name, description)");
        if (method.getReturnType() == void.class || method.getReturnType() == Void.class)
            throw new IllegalArgumentException("Typed execute must return a payload type; void needs a dedicated runtime adapter");
        if (!Modifier.isPublic(method.getParameterTypes()[0].getModifiers()))
            throw new IllegalArgumentException("The input record must be public for runtime binding");
        return new TypedReactorContract(method.getParameterTypes()[0].asSubclass(Record.class), method);
    }
}
