/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class TFrameid extends Token
{
    public TFrameid()
    {
        super.setText("f");
    }

    public TFrameid(int line, int pos)
    {
        super.setText("f");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TFrameid(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTFrameid(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TFrameid text.");
    }
}
