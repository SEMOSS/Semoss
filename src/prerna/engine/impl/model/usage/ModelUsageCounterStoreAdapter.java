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
package prerna.engine.impl.model.usage;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.impl.usage.IUsageCounterStore;
import prerna.engine.impl.usage.UsageCharge;
import prerna.engine.impl.usage.UsageHold;
import prerna.engine.impl.usage.UsageReservation;
import prerna.engine.impl.usage.UsageStoreException;

/** Keeps the model API stable while sharing the counter implementation. */
abstract class ModelUsageCounterStoreAdapter implements IModelUsageCounterStore {
	private final IUsageCounterStore delegate;

	ModelUsageCounterStoreAdapter(IUsageCounterStore delegate) {
		this.delegate = delegate;
	}

	@Override
	public Map<String, ModelUsageCount> get(Collection<String> keys) {
		try {
			Map<String, ModelUsageCount> result = new LinkedHashMap<>();
			delegate.get(keys).forEach((key, count) -> result.put(key,
					new ModelUsageCount(count.used(), count.reserved(), count.seeded())));
			return result;
		} catch (UsageStoreException e) {
			throw failure(e);
		}
	}

	@Override
	public ModelUsageReservation reserve(List<ModelUsageHold> holds, Duration expiry) {
		try {
			UsageReservation reservation = delegate.reserve(holds.stream().map(hold -> new UsageHold(hold.key(),
					hold.amount(), hold.limit(), hold.carried(), hold.expiresAtMillis())).toList(), expiry);
			return reservation == null ? null
					: new ModelUsageReservation(reservation.getExpiryMinute(), reservation.takeAll());
		} catch (UsageStoreException e) {
			throw failure(e);
		}
	}

	@Override
	public void settle(ModelUsageReservation reservation, Map<String, Long> released, List<ModelUsageCharge> charges) {
		try {
			delegate.settle(reservation, released, charges == null ? null
					: charges.stream()
							.map(charge -> new UsageCharge(charge.key(), charge.amount(), charge.expiresAtMillis()))
							.toList());
		} catch (UsageStoreException e) {
			throw failure(e);
		}
	}

	@Override
	public void seed(String key, long amount, long expiresAtMillis) {
		try {
			delegate.seed(key, amount, expiresAtMillis);
		} catch (UsageStoreException e) {
			throw failure(e);
		}
	}

	@Override
	public boolean markOnce(String key, Duration ttl) {
		try {
			return delegate.markOnce(key, ttl);
		} catch (UsageStoreException e) {
			throw failure(e);
		}
	}

	private static ModelUsageStoreException failure(UsageStoreException e) {
		return new ModelUsageStoreException(e.getMessage(), e);
	}
}
