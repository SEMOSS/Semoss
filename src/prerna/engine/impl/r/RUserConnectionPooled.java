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
package prerna.engine.impl.r;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SystemUtils;
import org.rosuda.REngine.REXP;

public class RUserConnectionPooled extends AbstractRUserConnection {

	private static final long ACTIVE_HEALTH_TIMEOUT = 12L; // TODO >>>timb: R - make this configurable in rdf map
	private static final TimeUnit ACTIVE_HEALTH_TIMEOUT_UNIT = TimeUnit.SECONDS;

	private final RserveConnectionMeta rconMeta;

	public RUserConnectionPooled(String rDataFile) {
		super(rDataFile);
		this.rconMeta = RserveConnectionPool.getInstance().getConnection();
		this.process = rconMeta.getProcess();
	}

	public RUserConnectionPooled() {
		super();
		this.rconMeta = RserveConnectionPool.getInstance().getConnection();
	}

	// Because windows reuses rcon, need to track when active
	// so we can moderate how long a user is allowed to block other user's execution
	// of r scripts
	@Override
	public REXP eval(String rScript) {
		if (SystemUtils.IS_OS_WINDOWS) {
			try {
				if (rconMeta.isActive()) {
					return super.eval(rScript, ACTIVE_HEALTH_TIMEOUT, ACTIVE_HEALTH_TIMEOUT_UNIT);
				} else {
					rconMeta.setActive(true);
					return super.eval(rScript);
				}
			} finally {
				rconMeta.setActive(false);
			}
		} else {
			return super.eval(rScript);
		}
	}

	@Override
	public void voidEval(String rScript) {
		if (SystemUtils.IS_OS_WINDOWS) {
			try {
				if (rconMeta.isActive()) {
					super.voidEval(rScript, ACTIVE_HEALTH_TIMEOUT, ACTIVE_HEALTH_TIMEOUT_UNIT);
				} else {
					rconMeta.setActive(true);
					super.voidEval(rScript);
				}
			} finally {
				rconMeta.setActive(false);
			}
		} else {
			super.voidEval(rScript);
		}
	}

	@Override
	public void initializeConnection() throws Exception {
		if (SystemUtils.IS_OS_WINDOWS) { // On windows, we need to recycle the rcon
			if (rconMeta.getRcon() != null) {
				rcon = rconMeta.getRcon();
			} else {
				reloadRcon();
				rconMeta.setRcon(rcon);
			}
		} else {
			reloadRcon();
		}
	}

	private void reloadRcon() throws Exception {
		if (rcon != null) {
			rcon.close(); // Close the old rcon and get a new one
		}
		rcon = RserveUtil.connect(rconMeta.getHost(), rconMeta.getPort());
	}

	@Override
	protected void recoverConnection() throws Exception {
		// First try to reestablish the connection without restarting Rserve itself
		try {
			initializeConnection();
			loadDefaultPackages();
		} catch (Exception e) {
			RserveConnectionPool.getInstance().recoverConnection(rconMeta);
			initializeConnection();
			loadDefaultPackages();
		}

		// Make sure R is healthy
		if (!isHealthy()) {
			throw new IllegalArgumentException("Basic R heath check failed after restarting R.");
		}
		this.stoppedR = false;
	}

	@Override
	public void stopR() throws Exception {
		if (rcon != null) {
			rcon.close();
		}
		RserveConnectionPool.getInstance().releaseConnection(rconMeta);
		this.stoppedR = true;
	}

	@Override
	public void cancelExecution() throws Exception {
		// TODO >>>timb: R - need to complete cancellation here (later)
	}

}
