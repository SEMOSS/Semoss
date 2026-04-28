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
package prerna.om;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.insight.InsightUtility;

/**
 * Utility for creating and cleaning up ephemeral child Insights used by sub-agents.
 *
 * <p>A forked Insight shares the parent's User and Project references but gets a fresh
 * insightId, VarStore, and PixelRunner to ensure complete isolation between concurrent
 * sub-agents.
 *
 * <p>Child insights are NOT added to the session hash (they are ephemeral infrastructure,
 * not user-visible insights).
 */
public final class InsightFork {

    private static final Logger logger = LogManager.getLogger(InsightFork.class);

    private InsightFork() { /* utility */ }

    /**
     * Forks the parent Insight to create an independent child execution context.
     * Registers the child in InsightStore but NOT in the session hash.
     */
    public static Insight forkForChildAgent(Insight parent) {
        Insight child = new Insight();
        child.setInsightId(UUID.randomUUID().toString());

        // Share user (read-only in child context) and project
        child.setUser(parent.getUser());
        if (parent.getProjectId() != null) {
            child.setProjectId(parent.getProjectId());
        }
        if (parent.getRdbmsId() != null) {
            child.setRdbmsId(parent.getRdbmsId());
        }

        // Register in InsightStore (NOT in session hash — ephemeral)
        InsightStore.getInstance().put(child);

        logger.debug("InsightFork: forked child insightId={} from parent insightId={}",
                child.getInsightId(), parent.getInsightId());
        return child;
    }

    /**
     * Cleans up a forked child Insight.
     * Safe to call even if the insight was never registered or already removed.
     */
    public static void cleanup(Insight child) {
        if (child == null) return;
        String childId = child.getInsightId();
        try {
            InsightUtility.clearInsight(child, false);
        } catch (Exception e) {
            logger.warn("InsightFork: error during clearInsight for child={}: {}", childId, e.getMessage());
        }
        InsightStore.getInstance().remove(childId);
        logger.debug("InsightFork: cleaned up child insightId={}", childId);
    }
}
