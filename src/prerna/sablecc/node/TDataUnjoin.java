/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class TDataUnjoin extends Token
{
    public TDataUnjoin()
    {
        super.setText("data.unjoin");
    }

    public TDataUnjoin(int line, int pos)
    {
        super.setText("data.unjoin");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TDataUnjoin(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTDataUnjoin(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TDataUnjoin text.");
    }
}
