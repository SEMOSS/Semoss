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
package prerna.reactor.frame;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PolarsFrame;
import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.reactor.ReactorFactory;
import prerna.reactor.frame.polars.ToUpperCaseReactor;

/**
 * Verifies explicit Polars creation, pandas alias compatibility, and
 * frame-specific reactor discovery.
 */
class FrameFactoryPolarsUnitTests {

	@Test
	void polarsIsExplicitAndExistingPythonAliasesRemainPandas() throws Exception {
		Insight insight = mock(Insight.class);
		PyTranslator translator = mock(PyTranslator.class);
		when(insight.getPyTranslator()).thenReturn(translator);

		assertInstanceOf(PolarsFrame.class, FrameFactory.getFrame(insight, "POLARS", "polarsFrame"));
		assertInstanceOf(PandasFrame.class, FrameFactory.getFrame(insight, "PY", "pyFrame"));
		assertInstanceOf(PandasFrame.class, FrameFactory.getFrame(insight, "PYTHON", "pythonFrame"));
		assertInstanceOf(PandasFrame.class, FrameFactory.getFrame(insight, "PYFRAME", "legacyPyFrame"));
		assertInstanceOf(PandasFrame.class, FrameFactory.getFrame(insight, "PANDAS", "pandasFrame"));
	}

	@Test
	void discoversIndirectPolarsReactors() {
		PolarsFrame frame = new PolarsFrame("polarsFrame", mock(PyTranslator.class));

		assertInstanceOf(ToUpperCaseReactor.class,
				ReactorFactory.getReactor("ToUpperCase", frame, null));
	}
}
