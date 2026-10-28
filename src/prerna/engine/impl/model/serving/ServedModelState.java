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
package prerna.engine.impl.model.serving;

/**
 * Where a served model (a KServe InferenceService) is in its lifecycle, as
 * derived by {@link ServedModelStateResolver} from the cluster's objects.
 */
public enum ServedModelState {

	/** Deleted; waiting for KServe's finalizer */
	REMOVING,
	/** Stop requested and no predictor pod left */
	STOPPED,
	/** Stop requested; the predictor pod is shutting down */
	STOPPING,
	/** A container failed or cannot start; see the failure */
	FAILED,
	/** The predictor pod cannot be scheduled because the GPU is taken */
	WAITING_FOR_GPU,
	/** On its way to READY; see the phase */
	STARTING,
	/** Serving requests */
	READY;

	/** @return whether the model holds, or is trying to get, the GPU */
	public boolean isActive() {
		return this == STARTING || this == WAITING_FOR_GPU || this == READY || this == FAILED || this == STOPPING;
	}
}
