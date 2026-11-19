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
package prerna.engine.impl.pipeline;

import java.util.Iterator;
import java.util.Set;

import prerna.util.SandboxedClassLoading;
import prerna.util.SemossClassLoader;

/**
 * Work an admin does as an admin. On an engine or project an admin holds only
 * their own permissions, like anyone else, except inside an admin operation:
 * there the engine proxy lets the platform's calls through for an admin
 * whatever they hold.
 *
 * <p>
 * An admin operation is running while a class that requires an admin is acting,
 * that is, while one is on the call stack: an admin reactor or admin endpoint,
 * which by convention is a class whose name starts with {@code Admin}, or
 * SecurityAdminUtils ({@link SemossClassLoader#requiresAdmin(String)}). So an
 * admin works on someone else's engine only through the admin reactors and
 * endpoints, which is what the screens of admin mode call, and a reactor every
 * user runs gives an admin what their own permissions give.
 *
 * <p>
 * Nothing begins or ends one: being an admin class is the declaration. An admin
 * operation never runs under a project's custom code, which cannot load an
 * admin class either, so an admin pixel a custom reactor runs through its
 * insight has the admin's own permissions only. A thread the work starts is
 * outside the operation.
 */
public final class AdminOperation {

	private static final StackWalker STACK = StackWalker
			.getInstance(Set.of(StackWalker.Option.RETAIN_CLASS_REFERENCE, StackWalker.Option.SHOW_HIDDEN_FRAMES));

	private AdminOperation() {
	}

	/**
	 * Whether the call being made on this thread is part of an admin operation. The
	 * caller still checks that the signed in user is an admin.
	 *
	 * @return true when a class that requires an admin is on the call stack and no
	 *         class of a project is
	 */
	public static boolean isRunning() {
		return STACK.walk(frames -> {
			boolean adminClassIsActing = false;
			Iterator<StackWalker.StackFrame> each = frames.iterator();
			while (each.hasNext()) {
				Class<?> frameClass = each.next().getDeclaringClass();
				if (frameClass.getNestHost() == AdminOperation.class) {
					continue;
				}
				if (SandboxedClassLoading.isProjectClass(frameClass)) {
					return false;
				}
				adminClassIsActing = adminClassIsActing || SemossClassLoader.requiresAdmin(frameClass.getName());
			}
			return adminClassIsActing;
		});
	}

}
