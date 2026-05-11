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
package prerna.sablecc2.om.execptions;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SemossPixelException extends RuntimeException implements ISemossException {

	private static final long serialVersionUID = 1L;

	protected boolean continueThreadOfExecution = true;
	protected NounMetadata noun = null;
	protected String message = null;

	public SemossPixelException(String message) {
		super(message);
		this.noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
	}

	public SemossPixelException(String message, Object errorBody) {
		super(message);
		this.noun = new NounMetadata(errorBody, PixelDataType.MAP, PixelOperationType.ERROR);
	}

	public SemossPixelException(String message, Throwable e) {
		super(message, e);
		this.noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
	}

	public SemossPixelException(Throwable cause) {
		super(cause);
		this.noun = new NounMetadata(cause.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
	}

	public SemossPixelException(String message, boolean continueThreadOfExecution) {
		super(message);
		this.noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		this.continueThreadOfExecution = continueThreadOfExecution;
	}

	public SemossPixelException(NounMetadata noun) {
		this.noun = noun;
		if (this.noun.getNounType() == PixelDataType.CONST_STRING || this.noun.getNounType() == PixelDataType.ERROR) {
			this.message = this.noun.getValue() + "";
		}
	}

	@Override
	public boolean isContinueThreadOfExecution() {
		return this.continueThreadOfExecution;
	}

	@Override
	public void setContinueThreadOfExecution(boolean continueThreadOfExecution) {
		this.continueThreadOfExecution = continueThreadOfExecution;
	}

	@Override
	public NounMetadata getNoun() {
		return this.noun;
	}

	@Override
	public String getMessage() {
		if (this.message == null) {
			return super.getMessage();
		}
		return this.message;
	}

}
