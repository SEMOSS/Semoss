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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); ...
 *******************************************************************************/
package prerna.reactor.agent.policy;

/**
 * Result of an {@link IAgentPolicy} check — what should happen and why.
 */
public final class PolicyDecision {

    public enum PolicyAction { ALLOW, BLOCK, ESCALATE_TO_HUMAN }

    private final boolean      allowed;
    private final String       reason;
    private final PolicyAction action;

    private PolicyDecision(boolean allowed, String reason, PolicyAction action) {
        this.allowed = allowed;
        this.reason  = reason;
        this.action  = action;
    }

    public boolean      isAllowed() { return allowed; }
    public String       getReason() { return reason;  }
    public PolicyAction getAction() { return action;  }

    public static PolicyDecision allow() {
        return new PolicyDecision(true, null, PolicyAction.ALLOW);
    }

    public static PolicyDecision block(String reason) {
        return new PolicyDecision(false, reason, PolicyAction.BLOCK);
    }

    public static PolicyDecision escalate(String reason) {
        return new PolicyDecision(false, reason, PolicyAction.ESCALATE_TO_HUMAN);
    }

    @Override
    public String toString() {
        return "PolicyDecision{action=" + action + ", reason=" + reason + '}';
    }
}
