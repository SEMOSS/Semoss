/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class TPlus extends Token
{
    public TPlus()
    {
        super.setText("+");
    }

    public TPlus(int line, int pos)
    {
        super.setText("+");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TPlus(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTPlus(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TPlus text.");
    }
}
