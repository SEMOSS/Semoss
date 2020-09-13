/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.Analysis;

@SuppressWarnings("nls")
public final class TDataconnectdbToken extends Token
{
    public TDataconnectdbToken()
    {
        super.setText("data.connectdb");
    }

    public TDataconnectdbToken(int line, int pos)
    {
        super.setText("data.connectdb");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TDataconnectdbToken(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTDataconnectdbToken(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TDataconnectdbToken text.");
    }
}
