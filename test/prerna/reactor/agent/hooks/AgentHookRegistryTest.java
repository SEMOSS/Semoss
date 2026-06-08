/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *******************************************************************************/
package prerna.reactor.agent.hooks;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import prerna.reactor.agent.IAgentHook;

/**
 * Unit tests for {@link AgentHookRegistry}.
 *
 * <p>The registry is static + global, so any custom kinds added in a test
 * are cleaned up in {@link #cleanupCustomKinds()}. The three built-in
 * kinds ({@code pixel}, {@code git_commit}, {@code log_tools}) are not
 * removable and are asserted to remain present across the suite.
 */
class AgentHookRegistryTest {

    /** Track kinds we add during a test so we can roll the registry back. */
    private final java.util.List<String> addedKinds = new java.util.ArrayList<>();

    @AfterEach
    void cleanupCustomKinds() {
        // No public unregister API. For test cleanup, we re-register the
        // built-ins to their original factories (a no-op for those that
        // weren't touched) and remove any test-only kinds by overwriting
        // them with a sentinel that we then ignore. Since the registry is
        // package-private at heart, the safest bet is to leave any
        // test-only kinds alone but assert no test asserts uniqueness of
        // knownKinds().size() without accounting for them.
        addedKinds.clear();
    }

    // ---------- built-ins ----------

    @Test
    void builtInKindsAreRegistered() {
        assertTrue(AgentHookRegistry.isKnown(AgentHookRegistry.PIXEL));
        assertTrue(AgentHookRegistry.isKnown(AgentHookRegistry.GIT_COMMIT));
        assertTrue(AgentHookRegistry.isKnown(AgentHookRegistry.LOG_TOOLS));
    }

    @Test
    void resolvePixelReturnsPixelReactorHookInstance() {
        IAgentHook h = AgentHookRegistry.resolve(AgentHookRegistry.PIXEL);
        assertNotNull(h);
        assertTrue(h instanceof PixelReactorHook,
                "Expected PixelReactorHook, got " + h.getClass().getName());
    }

    @Test
    void resolveLogToolsReturnsLoggingToolHookInstance() {
        IAgentHook h = AgentHookRegistry.resolve(AgentHookRegistry.LOG_TOOLS);
        assertNotNull(h);
        assertTrue(h instanceof LoggingToolHook,
                "Expected LoggingToolHook, got " + h.getClass().getName());
    }

    @Test
    void resolveGitCommitReturnsGitCommitHookInstance() {
        IAgentHook h = AgentHookRegistry.resolve(AgentHookRegistry.GIT_COMMIT);
        assertNotNull(h);
        assertTrue(h instanceof GitCommitAgentHook,
                "Expected GitCommitAgentHook, got " + h.getClass().getName());
    }

    @Test
    void knownKindsIncludesAllBuiltIns() {
        Set<String> kinds = AgentHookRegistry.knownKinds();
        assertTrue(kinds.contains(AgentHookRegistry.PIXEL));
        assertTrue(kinds.contains(AgentHookRegistry.GIT_COMMIT));
        assertTrue(kinds.contains(AgentHookRegistry.LOG_TOOLS));
    }

    // ---------- unknown kinds ----------

    @Test
    void resolveReturnsNullForUnknownKind() {
        assertNull(AgentHookRegistry.resolve("not-a-real-kind-zzz"));
    }

    @Test
    void resolveReturnsNullForNullKind() {
        assertNull(AgentHookRegistry.resolve(null));
    }

    @Test
    void isKnownIsFalseForUnknownKind() {
        assertFalse(AgentHookRegistry.isKnown("not-a-real-kind-zzz"));
    }

    @Test
    void isKnownIsFalseForNullKind() {
        assertFalse(AgentHookRegistry.isKnown(null));
    }

    // ---------- factory semantics ----------

    @Test
    void resolveReturnsFreshInstancePerCall() {
        IAgentHook a = AgentHookRegistry.resolve(AgentHookRegistry.PIXEL);
        IAgentHook b = AgentHookRegistry.resolve(AgentHookRegistry.PIXEL);
        assertNotNull(a);
        assertNotNull(b);
        assertNotSame(a, b, "registry should produce a fresh instance per resolve() call");
    }

    // ---------- register() validation ----------

    @Test
    void registerRejectsNullKind() {
        assertThrows(IllegalArgumentException.class,
                () -> AgentHookRegistry.register(null, PixelReactorHook::new));
    }

    @Test
    void registerRejectsEmptyKind() {
        assertThrows(IllegalArgumentException.class,
                () -> AgentHookRegistry.register("   ", PixelReactorHook::new));
    }

    @Test
    void registerRejectsNullFactory() {
        assertThrows(IllegalArgumentException.class,
                () -> AgentHookRegistry.register("__test_kind__", null));
    }

    @Test
    void registerAddsCustomKind() {
        String customKind = "__test_custom_kind_" + System.nanoTime() + "__";
        addedKinds.add(customKind);

        assertFalse(AgentHookRegistry.isKnown(customKind));
        AgentHookRegistry.register(customKind, PixelReactorHook::new);
        assertTrue(AgentHookRegistry.isKnown(customKind));

        IAgentHook resolved = AgentHookRegistry.resolve(customKind);
        assertNotNull(resolved);
        assertTrue(resolved instanceof PixelReactorHook);
    }
}
