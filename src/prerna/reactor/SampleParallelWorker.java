package prerna.reactor;

import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SampleParallelWorker implements IParallelWorker {

	Insight insight = null;
	@Override
	public void setInisight(Insight insight) {
		// TODO Auto-generated method stub
		this.insight = insight;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		for(int idx = 0;idx < 5;idx++)
		{
			insight.getVarStore().put("par1", new NounMetadata(idx, PixelDataType.CONST_STRING));
			try
			{
				Thread.sleep(2000);
				System.out.println("Setting insight.. var to  " + idx);
			}catch(Exception ex)
			{
				
			}
		}

	}

}
