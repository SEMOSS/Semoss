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
import prerna.auth.access.EngineGrantee;
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
import prerna.util.SandboxedClassLoading;
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
 * function engine opening its storage engine. The engine is the one answering
 * the proxy call that runs on the thread, or the one being opened
 * ({@link EngineCallPermission}), so the calls an engine makes from its
 * {@code open} are its own. A reactor a user runs inside an engine's call, such
 * as a tool of a project, is the platform again. An engine object a project's
 * custom code creates or extends and calls itself, with no proxy in between,
 * acts as that project's custom code.</li>
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
 * <b>An engine</b> calling another engine needs the method's level for the
 * signed in user, or a permission the other engine gives it that covers the
 * level ({@code ENGINE2ENGINEPERMISSION}). Without one, it goes through only when
 * it calls itself, when the engine answering is a project, when the other engine
 * is a venv engine, when the method needs READ_ONLY and the other engine is
 * public, or as a platform call does below, except for reading a discoverable
 * engine. An engine calling a project is checked as the platform is.
 *
 * <p>
 * <b>The platform</b> needs the method's level for the signed in user. The call
 * also goes through when:
 * <ul>
 * <li>the platform is loading;</li>
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
	 * loading, whoever asked for the engine, except a call the engine being opened
	 * makes from its {@code open}, which is that engine's.
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
	 * @param catalogType    its catalog type, {@code PROJECT} for a project
	 * @param forPlatformUse true when the proxy was handed out for the platform's
	 *                       own use
	 * @param method         the interface method being called
	 * @param args           the call's arguments, or null
	 * @return the app or engine making the call, when the call is an app's custom
	 *         code's or an engine's own code's and the engine gives that app or
	 *         engine a permission; null for every other call
	 * @throws SemossPixelException if the call is not allowed
	 */
	static EngineGrantee check(String engineId, IEngine.CATALOG_TYPE catalogType, boolean forPlatformUse,
			Method method, Object[] args) {
		boolean project = catalogType == IEngine.CATALOG_TYPE.PROJECT;
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
			return project ? null : granteeMakingTheCall(engineId);
		}

		AccessPermissionEnum required = levels.forEveryCall();
		// A project's custom code calling the method itself needs the level for project
		// code, which is higher on the methods that hand back a connection or the
		// engine's settings
		if (levels.forProjectCode() != levels.forEveryCall() && directCallerIsProjectCode()) {
			required = levels.forProjectCode();
		}
		// The user holds the level this call needs
		if (covers(held, required)) {
			return project ? null : granteeMakingTheCall(engineId);
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
				return project ? null : EngineGrantee.ofApp(caller.projectId());
			}
			throw refused(engineId, project, method, required, user, caller);
		}
		// The platform loading an engine or project, a proxy handed out for the
		// platform's own use, or the platform's own work for no user
		if (caller.kind() == CallerKind.ENGINE_LOADER || forPlatformUse || PlatformOperation.covering() != null) {
			return null;
		}
		boolean engineIsCalling = caller.kind() == CallerKind.ENGINE && !project;
		if (engineIsCalling) {
			EngineGrantee calling = caller.engine();
			if (calling != null && !calling.isApp()
					&& covers(SecurityEngineAccessUtils.getGranteePermission(calling, engineId), required)) {
				return calling;
			}
			if (passesWithoutAGrant(calling, engineId, catalogType, required)) {
				return null;
			}
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
		if (!engineIsCalling && required == AccessPermissionEnum.READ_ONLY
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
	 * @return the level every call needs and the level for a call a project's
	 *         custom code makes itself
	 */
	static RequiredLevels requiredLevelsOf(Method method) {
		EngineAccess access = method.getAnnotation(EngineAccess.class);
		if (access != null) {
			AccessPermissionEnum level = access.value();
			AccessPermissionEnum[] projectCode = access.projectCode();
			return new RequiredLevels(level, projectCode.length == 0 ? level : projectCode[0]);
		}
		if (method.getDeclaringClass() == IReactor.class) {
			return READ_ONLY_LEVELS;
		}
		return CLOSE.equals(method.getName()) && method.getParameterCount() == 0 ? CLOSE_LEVELS : OWNER_LEVELS;
	}

	/**
	 * The app or engine making a call the signed in user holds the level for. Most
	 * engines give no app or engine a permission, and the call stack is read only
	 * on one that does.
	 *
	 * @param engineId the engine being called
	 * @return the app whose custom code, or the engine whose own code, is calling,
	 *         when the engine gives it a permission; null otherwise
	 */
	private static EngineGrantee granteeMakingTheCall(String engineId) {
		if (!SecurityEngineAccessUtils.givesAGranteePermission(engineId)) {
			return null;
		}
		Caller caller = Caller.fromStack();
		EngineGrantee grantee = null;
		if (caller.kind() == CallerKind.PROJECT_CODE && caller.projectId() != null) {
			grantee = EngineGrantee.ofApp(caller.projectId());
		} else if (caller.kind() == CallerKind.ENGINE && caller.engine() != null && !caller.engine().isApp()) {
			grantee = caller.engine();
		}
		if (grantee == null || SecurityEngineAccessUtils.getGranteePermission(grantee, engineId) == null) {
			return null;
		}
		return grantee;
	}

	/**
	 * Whether an engine's call to another engine goes through with no permission
	 * the other engine gives it: the engine calls itself, the engine answering is a
	 * project, whose own code is the platform's, the other engine is a venv engine,
	 * which only hands out where its Python runs, or the method needs READ_ONLY and
	 * the other engine is public, as everyone may read it.
	 *
	 * @param calling     the engine calling, or null when the engine acting is not
	 *                    known
	 * @param engineId    the engine called
	 * @param catalogType its catalog type
	 * @param required    the level the call needs
	 * @return whether the call goes through
	 */
	private static boolean passesWithoutAGrant(EngineGrantee calling, String engineId,
			IEngine.CATALOG_TYPE catalogType, AccessPermissionEnum required) {
		if (calling != null && (calling.isApp() || engineId.equals(calling.engineId()))) {
			return true;
		}
		if (catalogType == IEngine.CATALOG_TYPE.VENV) {
			return true;
		}
		return required == AccessPermissionEnum.READ_ONLY && SecurityEngineAccessUtils.isPublic(engineId);
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
		return SecurityEngineAccessUtils.getGranteePermission(EngineGrantee.ofApp(projectId), engineId);
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
			return directCaller != null && SandboxedClassLoading.isProjectClass(directCaller.getDeclaringClass());
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
			if (SandboxedClassLoading.isProjectClass(frameClass) || isPlatform(frameClass)) {
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
			classLogger.info(
					"Refused {} on {}{}: needs {}, user signed in = {}, calling project = {}, calling engine = {}, "
							+ "called from {}",
					method.getName(), kind, engineId, level, user != null, caller.projectId(),
					caller.engine() == null ? null : caller.engine().engineId(), caller.className());
		}
		if (caller.kind() == CallerKind.PROJECT_CODE && user == null) {
			return new SemossPixelException("A custom reactor needs a signed in user to call " + method.getName()
					+ " on " + kind + engineId);
		}
		if (caller.kind() == CallerKind.PROJECT_CODE) {
			return new SemossPixelException(kind + engineId + " does not exist or neither the user nor the app has "
					+ level + " permission on it, which a custom reactor needs to call " + method.getName());
		}
		if (caller.kind() == CallerKind.ENGINE && !project) {
			String calling = caller.engine() == null ? "the calling engine" : "engine " + caller.engine().engineId();
			return new SemossPixelException(kind + engineId + " does not exist or neither the user nor " + calling
					+ " has " + level + " permission on it");
		}
		return new SemossPixelException(
				kind + engineId + " does not exist or user does not have " + level + " permission on it");
	}

	/**
	 * The levels of one method.
	 *
	 * @param forEveryCall   what every call needs, whoever makes it
	 * @param forProjectCode what is needed when a project's custom code makes the
	 *                       call itself
	 */
	record RequiredLevels(AccessPermissionEnum forEveryCall, AccessPermissionEnum forProjectCode) {
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
	 * @param engine    the engine acting, when an engine is: the one answering the
	 *                  proxy call nearest on the stack, or the one being opened;
	 *                  null when that is not known
	 * @param className the name of the first platform or project class below the
	 *                  proxy, for the log
	 */
	private record Caller(CallerKind kind, String projectId, EngineGrantee engine, String className) {

		static Caller fromStack() {
			return STACK.walk(frames -> {
				Iterator<StackWalker.StackFrame> each = frames.iterator();
				StackWalker.StackFrame frame = firstCallerFrame(each);
				String className = frame == null ? null : frame.getDeclaringClass().getName();
				boolean anEngineIsActing = false;
				while (frame != null) {
					Class<?> frameClass = frame.getDeclaringClass();
					if (SandboxedClassLoading.isProjectClass(frameClass)) {
						return new Caller(CallerKind.PROJECT_CODE, SandboxedClassLoading.projectIdOf(frameClass), null,
								className);
					}
					if (frameClass == PipelineInvocationHandler.class) {
						return new Caller(CallerKind.ENGINE, null, EngineCallPermission.actingEngine(), className);
					}
					if (isLoader(frame)) {
						return anEngineIsActing && EngineCallPermission.isOpening()
								? new Caller(CallerKind.ENGINE, null, EngineCallPermission.actingEngine(), className)
								: new Caller(CallerKind.ENGINE_LOADER, null, null, className);
					}
					if (IEngine.class.isAssignableFrom(frameClass) && !Proxy.isProxyClass(frameClass)) {
						anEngineIsActing = true;
					} else if (isReactorTheUserRuns(frameClass)) {
						break;
					}
					frame = each.hasNext() ? each.next() : null;
				}
				return new Caller(anEngineIsActing ? CallerKind.ENGINE : CallerKind.PLATFORM, null, null, className);
			});
		}
	}

}
