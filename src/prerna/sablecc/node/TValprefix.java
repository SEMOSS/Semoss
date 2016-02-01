/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class TValprefix extends Token
{
    public TValprefix()
    {
        super.setText("v:");
    }

    public TValprefix(int line, int pos)
    {
        super.setText("v:");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TValprefix(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTValprefix(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TValprefix text.");
    }
}
