/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class TCol extends Token
{
    public TCol()
    {
        super.setText("col");
    }

    public TCol(int line, int pos)
    {
        super.setText("col");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TCol(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTCol(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TCol text.");
    }
}
