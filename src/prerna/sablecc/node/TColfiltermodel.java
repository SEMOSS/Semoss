/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class TColfiltermodel extends Token
{
    public TColfiltermodel()
    {
        super.setText("col.filterModel");
    }

    public TColfiltermodel(int line, int pos)
    {
        super.setText("col.filterModel");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TColfiltermodel(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTColfiltermodel(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TColfiltermodel text.");
    }
}
