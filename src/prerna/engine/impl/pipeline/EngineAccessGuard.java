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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineAccessUtils;
import prerna.engine.api.EngineAccess;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.ThreadStore;
import prerna.reactor.IReactor;
import prerna.reactor.interceptor.IInputReactor;
import prerna.reactor.interceptor.IOutputReactor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.InternalUtility;
import prerna.util.SandboxedJavaExecution;
import prerna.util.Utility;

/**
 * Checks that a call to an engine or project method is allowed before the
 * engine proxy passes it on. Every method of the engine and project interfaces
 * carries an {@link EngineAccess} level. Nothing turns the check off.
 *
 * <p>
 * <b>Who is acting</b> is read from the call stack, from the proxy down:
 * <ul>
 * <li><b>A project's custom code</b>, when one of its classes comes first. The
 * classes of the JDK and of shared libraries are passed over, so a call a
 * project makes through a lambda, a stream or a library is still the
 * project's.</li>
 * <li><b>An engine</b>, when an engine's own class or another proxy call comes
 * first: a vector database calling the model that makes its embeddings, or a
 * function engine opening its storage engine. A reactor a user runs inside an
 * engine's call, such as a tool of a project, is the platform again.</li>
 * <li><b>The platform loading</b> an engine or project, when the getter that
 * loads it comes first.</li>
 * <li><b>The platform</b> otherwise.</li>
 * </ul>
 *
 * <p>
 * <b>A project's custom code</b> needs the method's level, and the level given
 * for project code when it makes the call itself. It has the level when the
 * user signed in on the thread holds it, or when the engine gave the project a
 * permission that covers it. On its own project it has EDIT. It runs only for
 * the user signed in on the thread, never for the user of an insight or room it
 * passes, and with no signed in user it is refused. Being an admin gives it
 * nothing.
 *
 * <p>
 * <b>The platform</b> needs the method's level for the signed in user. The call
 * also goes through when:
 * <ul>
 * <li>an engine is acting, or the platform is loading;</li>
 * <li>the proxy was handed out for the platform's own use
 * ({@link InternalUtility#getPlatformModel(String)});</li>
 * <li>the platform is running a {@link PlatformOperation}, its own work for no
 * user, such as a cluster sync;</li>
 * <li>the method needs READ_ONLY and the platform may read the engine or project
 * for the user: a discoverable one, or a project with an insight the user can
 * open;</li>
 * <li>the user is an admin and an {@link AdminOperation} is running: an admin
 * reactor or endpoint is acting;</li>
 * <li>the security database does not list the engine or project, which is so
 * only while it is created or deleted.</li>
 * </ul>
 * A call the platform makes with no signed in user, outside a platform
 * operation, is refused.
 */
final class EngineAccessGuard {

	private static final Logger classLogger = LogManager.getLogger(EngineAccessGuard.class);

	private static final String PLATFORM_PACKAGE = "prerna.";
	private static final String CLOSE = "close";

	/**
	 * The methods of {@link Utility} and {@link InternalUtility} that load an
	 * engine or project. A proxy call made while one of them runs is the platform
	 * loading, whoever asked for the engine.
	 */
	private static final Set<String> LOADERS = Set.of("getProject", "getEngine", "getDatabase", "getStorage",
			"getModel", "getVectorDatabase", "getReactorEngine", "getFunctionEngine", "getGuardrailEngine",
			"getVenvEngine", "loadProject", "loadEngine", "baseGetEngine");

	private static final StackWalker STACK = StackWalker.getInstance(
			Set.of(StackWalker.Option.RETAIN_CLASS_REFERENCE, StackWalker.Option.SHOW_HIDDEN_FRAMES));

	private static final RequiredLevels READ_ONLY_LEVELS = new RequiredLevels(AccessPermissionEnum.READ_ONLY,
			AccessPermissionEnum.READ_ONLY);
	private static final RequiredLevels OWNER_LEVELS = new RequiredLevels(AccessPermissionEnum.OWNER,
			AccessPermissionEnum.OWNER);
	private static final RequiredLevels CLOSE_LEVELS = new RequiredLevels(AccessPermissionEnum.READ_ONLY,
			AccessPermissionEnum.OWNER);

	private static final Map<Method, RequiredLevels> REQUIRED_LEVELS = new ConcurrentHashMap<>();

	private EngineAccessGuard() {
	}

	/**
	 * Refuses a call the caller has no permission for.
	 *
	 * @param engineId       the id of the engine or project the proxy stands for
	 * @param project        true when the proxy stands for a project
	 * @param forPlatformUse true when the proxy was handed out for the platform's
	 *                       own use
	 * @param method         the interface method being called
	 * @param args           the call's arguments, or null
	 * @return the project id of the app making the call, when the call is a
	 *         project's custom code's and the engine gives that project a
	 *         permission; null for every other call
	 * @throws SemossPixelException if the call is not allowed
	 */
	static String check(String engineId, boolean project, boolean forPlatformUse, Method method, Object[] args) {
		// Methods of Object, such as toString, are not checked, and neither is a JVM
		// without the security database: the Java worker's engines send each call back
		// to the server, which checks it there
		if (method.getDeclaringClass() == Object.class || !SecurityEngineAccessUtils.isSecurityDatabaseLoaded()) {
			return null;
		}
		RequiredLevels levels = REQUIRED_LEVELS.computeIfAbsent(method, EngineAccessGuard::requiredLevelsOf);

		User user = ThreadStore.getUser();
		AccessPermissionEnum held = user == null ? null
				: SecurityEngineAccessUtils.getUserPermission(user, engineId, project);
		// The user holds the higher of the method's two levels, so the call goes
		// through whoever makes it, without reading the call stack
		if (covers(held, levels.forProjectCode())) {
			return project ? null : appMakingTheCall(engineId);
		}

		AccessPermissionEnum required = levels.forUser();
		// A project's custom code calling the method itself needs the level for project
		// code, which is higher on the methods that hand back a connection or the
		// engine's settings
		if (levels.forProjectCode() != levels.forUser() && directCallerIsProjectCode()) {
			required = levels.forProjectCode();
		}
		// The user holds the level this call needs
		if (covers(held, required)) {
			return project ? null : appMakingTheCall(engineId);
		}

		Caller caller = Caller.fromStack();
		// A project's custom code is acting: it has the signed in user's level, which
		// did not cover the call, or the permission the engine gives its app
		if (caller.kind() == CallerKind.PROJECT_CODE) {
			// Custom code runs only for the user signed in on the thread
			if (user == null) {
				throw refused(engineId, project, method, required, null, caller);
			}
			// The engine gives the app a permission that covers the call, or the call is
			// on the app's own project
			if (covers(projectPermission(caller.projectId(), engineId, project), required)) {
				return project ? null : caller.projectId();
			}
			throw refused(engineId, project, method, required, user, caller);
		}
		// An engine calling the engine configured on it, the platform loading an engine
		// or project, a proxy handed out for the platform's own use, or the platform's
		// own work for no user
		if (caller.kind() != CallerKind.PLATFORM || forPlatformUse || PlatformOperation.covering() != null) {
			return null;
		}
		// A pooled thread names its user through the insight or room of the call, read
		// only when no project's custom code is anywhere on the stack
		if (user == null && !PlatformOperation.projectCodeIsOnTheStack()) {
			user = signedInUser(args);
			held = user == null ? null : SecurityEngineAccessUtils.getUserPermission(user, engineId, project);
			// The user of the call holds the level this call needs
			if (covers(held, required)) {
				return null;
			}
		}
		// The platform made the call for no user, outside a platform operation
		if (user == null) {
			throw refused(engineId, project, method, required, null, caller);
		}
		// Anyone may read a discoverable engine or project, or a project with an
		// insight they can open, through the platform
		if (required == AccessPermissionEnum.READ_ONLY
				&& SecurityEngineAccessUtils.isReadableThroughPlatform(user, engineId, project)) {
			return null;
		}
		// The engine or project is being created or deleted, so the security database
		// does not list it
		if (!SecurityEngineAccessUtils.isListed(engineId, project)) {
			return null;
		}
		// An admin, while an admin reactor or endpoint is acting
		if (SecurityEngineAccessUtils.isAdmin(user) && AdminOperation.isRunning()) {
			return null;
		}
		throw refused(engineId, project, method, required, user, caller);
	}

	/**
	 * The level a method needs: the one its {@link EngineAccess} gives. A method
	 * with none is one the proxy takes from an interface that is not an engine's
	 * own. The reactor methods of a function engine need READ_ONLY. Closing needs
	 * READ_ONLY from the platform, which closes an engine to sync its files, and
	 * OWNER from a project's custom code. Any other method needs OWNER.
	 *
	 * @param method an interface method of the proxy
	 * @return the level for the signed in user and the level for project code
	 */
	static RequiredLevels requiredLevelsOf(Method method) {
		EngineAccess access = method.getAnnotation(EngineAccess.class);
		if (access != null) {
			AccessPermissionEnum user = access.value();
			AccessPermissionEnum[] projectCode = access.projectCode();
			return new RequiredLevels(user, projectCode.length == 0 ? user : projectCode[0]);
		}
		if (method.getDeclaringClass() == IReactor.class) {
			return READ_ONLY_LEVELS;
		}
		return CLOSE.equals(method.getName()) && method.getParameterCount() == 0 ? CLOSE_LEVELS : OWNER_LEVELS;
	}

	/**
	 * The app making a call the signed in user holds the level for. Most engines
	 * give no app a permission, and the call stack is read only on one that does.
	 *
	 * @param engineId the engine being called
	 * @return the project id of the app whose custom code is calling, when the
	 *         engine gives that app a permission; null otherwise
	 */
	private static String appMakingTheCall(String engineId) {
		if (!SecurityEngineAccessUtils.givesAProjectPermission(engineId)) {
			return null;
		}
		Caller caller = Caller.fromStack();
		if (caller.kind() != CallerKind.PROJECT_CODE
				|| SecurityEngineAccessUtils.getProjectPermission(caller.projectId(), engineId) == null) {
			return null;
		}
		return caller.projectId();
	}

	private static boolean covers(AccessPermissionEnum held, AccessPermissionEnum required) {
		return held != null && held.getId() <= required.getId();
	}

	private static AccessPermissionEnum projectPermission(String projectId, String engineId, boolean project) {
		if (projectId == null) {
			return null;
		}
		if (project) {
			return projectId.equals(engineId) ? AccessPermissionEnum.EDIT : null;
		}
		return SecurityEngineAccessUtils.getProjectPermission(projectId, engineId);
	}

	/**
	 * The user a call the platform makes on a thread with no signed in user is made
	 * for: the user of the insight or room the call carries, or else of the insight
	 * the thread names, which is how a pooled thread names its user. A project's
	 * custom code has only the user signed in on the thread.
	 *
	 * @param args the call's arguments, or null
	 * @return the user, or null when the call is made for none
	 */
	private static User signedInUser(Object[] args) {
		Insight insight = null;
		if (args != null) {
			for (Object arg : args) {
				if (arg instanceof Insight) {
					insight = (Insight) arg;
					break;
				}
				if (arg instanceof Room room && room.getInsight() != null) {
					insight = room.getInsight();
					break;
				}
			}
		}
		if (insight == null) {
			String insightId = ThreadStore.getInsightId();
			insight = insightId == null ? null : InsightStore.getInstance().get(insightId);
		}
		return insight == null ? null : insight.getUser();
	}

	private static boolean directCallerIsProjectCode() {
		return STACK.walk(frames -> {
			StackWalker.StackFrame directCaller = firstCallerFrame(frames.iterator());
			return directCaller != null && SandboxedJavaExecution.isProjectClass(directCaller.getDeclaringClass());
		});
	}

	/**
	 * Moves past the guard, the proxy handler and the proxy, then past the classes
	 * of the JDK and of shared libraries, to the first frame of a class that is the
	 * platform's or a project's.
	 *
	 * @param frames the frames of the stack, from the top
	 * @return that frame, or null when the stack has none
	 */
	private static StackWalker.StackFrame firstCallerFrame(Iterator<StackWalker.StackFrame> frames) {
		int passed = 0;
		while (frames.hasNext()) {
			StackWalker.StackFrame frame = frames.next();
			Class<?> frameClass = frame.getDeclaringClass();
			if (passed == 0) {
				if (isGuard(frameClass) || frameClass.getClassLoader() == null) {
					continue;
				}
				passed = 1;
			}
			if (passed == 1) {
				if (frameClass == PipelineInvocationHandler.class) {
					continue;
				}
				passed = 2;
			}
			if (passed == 2) {
				if (Proxy.isProxyClass(frameClass)) {
					continue;
				}
				passed = 3;
			}
			if (SandboxedJavaExecution.isProjectClass(frameClass) || isPlatform(frameClass)) {
				return frame;
			}
		}
		return null;
	}

	private static boolean isGuard(Class<?> candidate) {
		return candidate == EngineAccessGuard.class || candidate.getNestHost() == EngineAccessGuard.class;
	}

	private static boolean isPlatform(Class<?> candidate) {
		return candidate.getName().startsWith(PLATFORM_PACKAGE);
	}

	/**
	 * Whether a class is a reactor a user runs, which ends the search for an engine
	 * that is acting: what such a reactor does inside an engine's call, as a tool
	 * of a project or a function engine does, is done for the user. The reactors of
	 * an engine's own pipeline are not, since the engine runs them on every call.
	 *
	 * @param candidate a platform class
	 * @return whether the class is a reactor outside an engine's pipeline
	 */
	private static boolean isReactorTheUserRuns(Class<?> candidate) {
		return IReactor.class.isAssignableFrom(candidate) && !IInputReactor.class.isAssignableFrom(candidate)
				&& !IOutputReactor.class.isAssignableFrom(candidate);
	}

	private static boolean isLoader(StackWalker.StackFrame frame) {
		Class<?> frameClass = frame.getDeclaringClass();
		return (frameClass == Utility.class || frameClass == InternalUtility.class)
				&& LOADERS.contains(frame.getMethodName());
	}

	private static SemossPixelException refused(String engineId, boolean project, Method method,
			AccessPermissionEnum required, User user, Caller caller) {
		String kind = project ? "Project " : "Engine ";
		String level = required.getPermission();
		if (user == null && caller.kind() == CallerKind.PLATFORM) {
			classLogger.warn("Refused {} on {}{}: the platform made the call with no signed in user and outside a "
					+ "platform operation, from {}", method.getName(), kind, engineId, caller.className());
		} else {
			classLogger.info("Refused {} on {}{}: needs {}, user signed in = {}, calling project = {}, called from {}",
					method.getName(), kind, engineId, level, user != null, caller.projectId(), caller.className());
		}
		if (caller.kind() == CallerKind.PROJECT_CODE && user == null) {
			return new SemossPixelException("A custom reactor needs a signed in user to call " + method.getName()
					+ " on " + kind + engineId);
		}
		if (caller.kind() == CallerKind.PROJECT_CODE) {
			return new SemossPixelException(kind + engineId + " does not exist or neither the user nor the app has "
					+ level + " permission on it, which a custom reactor needs to call " + method.getName());
		}
		return new SemossPixelException(
				kind + engineId + " does not exist or user does not have " + level + " permission on it");
	}

	/**
	 * The levels of one method.
	 *
	 * @param forUser        what the signed in user needs
	 * @param forProjectCode what is needed when a project's custom code makes the
	 *                       call itself
	 */
	record RequiredLevels(AccessPermissionEnum forUser, AccessPermissionEnum forProjectCode) {
	}

	/**
	 * Whose work a call is.
	 */
	private enum CallerKind {
		PROJECT_CODE, ENGINE, ENGINE_LOADER, PLATFORM
	}

	/**
	 * What the call stack says about who is acting.
	 *
	 * @param kind      whose work the call is
	 * @param projectId the project whose custom code is calling, or null
	 * @param className the name of the first platform or project class below the
	 *                  proxy, for the log
	 */
	private record Caller(CallerKind kind, String projectId, String className) {

		static Caller fromStack() {
			return STACK.walk(frames -> {
				Iterator<StackWalker.StackFrame> each = frames.iterator();
				StackWalker.StackFrame frame = firstCallerFrame(each);
				String className = frame == null ? null : frame.getDeclaringClass().getName();
				while (frame != null) {
					Class<?> frameClass = frame.getDeclaringClass();
					if (SandboxedJavaExecution.isProjectClass(frameClass)) {
						return new Caller(CallerKind.PROJECT_CODE, SandboxedJavaExecution.projectIdOf(frameClass), className);
					}
					if (frameClass == PipelineInvocationHandler.class
							|| (IEngine.class.isAssignableFrom(frameClass) && !Proxy.isProxyClass(frameClass))) {
						return new Caller(CallerKind.ENGINE, null, className);
					}
					if (isLoader(frame)) {
						return new Caller(CallerKind.ENGINE_LOADER, null, className);
					}
					if (isReactorTheUserRuns(frameClass)) {
						break;
					}
					frame = each.hasNext() ? each.next() : null;
				}
				return new Caller(CallerKind.PLATFORM, null, className);
			});
		}
	}

}
