/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class TSpace extends Token
{
    public TSpace(String text)
    {
        setText(text);
    }

    public TSpace(String text, int line, int pos)
    {
        setText(text);
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TSpace(getText(), getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTSpace(this);
    }
}
