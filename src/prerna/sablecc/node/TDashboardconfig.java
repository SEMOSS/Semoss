/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class TDashboardconfig extends Token
{
    public TDashboardconfig()
    {
        super.setText("dashboard.config");
    }

    public TDashboardconfig(int line, int pos)
    {
        super.setText("dashboard.config");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TDashboardconfig(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTDashboardconfig(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TDashboardconfig text.");
    }
}
