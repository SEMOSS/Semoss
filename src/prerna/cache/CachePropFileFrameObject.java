/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.cache;

public class CachePropFileFrameObject {

	// the cache location of the frame
	private String frameCacheLocation;
	// the cache location of the metadata
	private String frameMetaCacheLocation;
	// the cache location of frame state variables
	private String frameStateCacheLocation;

	// the frame name
	private String frameName;
	// the frame type
	private String frameType;

	public CachePropFileFrameObject() {
	}

	public String getFrameCacheLocation() {
		return frameCacheLocation;
	}

	public void setFrameCacheLocation(String frameCacheLocation) {
		this.frameCacheLocation = frameCacheLocation;
	}

	public String getFrameMetaCacheLocation() {
		return frameMetaCacheLocation;
	}

	public void setFrameMetaCacheLocation(String frameMetaCacheLocation) {
		this.frameMetaCacheLocation = frameMetaCacheLocation;
	}

	public String getFrameStateCacheLocation() {
		return frameStateCacheLocation;
	}

	public void setFrameStateCacheLocation(String frameStateCacheLocation) {
		this.frameStateCacheLocation = frameStateCacheLocation;
	}

	public String getFrameName() {
		return frameName;
	}

	public void setFrameName(String frameName) {
		this.frameName = frameName;
	}

	public String getFrameType() {
		return frameType;
	}

	public void setFrameType(String frameType) {
		this.frameType = frameType;
	}
}
