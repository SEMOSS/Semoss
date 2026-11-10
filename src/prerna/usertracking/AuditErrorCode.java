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
package prerna.usertracking;

/**
 * Why an audited action did not succeed, stored in
 * {@code USER_AUDIT_EVENTS.ERROR_CODE} beside the sanitized message.
 */
public enum AuditErrorCode {

	/** The actor lacks the permission the action needs */
	PERMISSION_DENIED,
	/** The action is for admins only */
	ADMIN_REQUIRED,
	/** The action needs a signed in user */
	LOGIN_REQUIRED,
	/** What the action is on does not exist */
	NOT_FOUND,
	/** The input to the action is not valid */
	VALIDATION_FAILED,
	/** The action did not complete */
	OPERATION_FAILED,
	/** The action stopped on an unexpected error */
	INTERNAL_ERROR,
	/** The user name or password is wrong */
	INVALID_CREDENTIALS,
	/** The user name or password is missing */
	MISSING_CREDENTIALS,
	/** The account is locked */
	ACCOUNT_LOCKED,
	/** The user must change their password before logging in */
	PASSWORD_CHANGE_REQUIRED,
	/** The login method is turned off */
	LOGIN_METHOD_DISABLED,
	/** The user may not read the resource's audit trail */
	AUDIT_ACCESS_DENIED,
	/** The audit trail could not be read */
	AUDIT_READ_FAILED,
	/** The audit trail's chain of hashes could not be verified */
	AUDIT_INTEGRITY_CHECK_FAILED,
	/** An automation run failed */
	AUTOMATION_FAILED,
	/** An automation run was cancelled */
	AUTOMATION_CANCELLED,
	/** An automation run was interrupted, such as by a restart */
	AUTOMATION_INTERRUPTED;
}
