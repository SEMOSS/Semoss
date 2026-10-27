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
 * The steps of {@link ServedModelState#STARTING}, in the order they happen.
 *
 * Each carries the default length used for the rough progress and ETA. The
 * defaults are this box's measured times for a small model (OLMo-2 1B with the
 * vLLM image cached: about 37-140 s to download 3 GB from Hugging Face, then
 * about 75 s from container start to "Application startup complete").
 */
public enum ServedModelPhase {

	CREATING("Creating the model server pod", 5),
	PULLING_INIT_IMAGE("Pulling the storage-initializer image", 30),
	DOWNLOADING_WEIGHTS("Downloading the model weights", 120),
	PULLING_IMAGE("Pulling the model server image (several minutes on the first start on this node)", 480),
	LOADING_WEIGHTS("Starting vLLM and loading the weights into GPU memory", 35),
	COMPILING("Compiling the model (torch.compile)", 15),
	WARMING("Profiling memory and capturing CUDA graphs", 30),
	FINISHING("Starting the API server", 10);

	private final String message;
	private final int defaultSeconds;

	ServedModelPhase(String message, int defaultSeconds) {
		this.message = message;
		this.defaultSeconds = defaultSeconds;
	}

	public String getMessage() {
		return this.message;
	}

	public int getDefaultSeconds() {
		return this.defaultSeconds;
	}
}
