/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class TAndComparator extends Token
{
    public TAndComparator()
    {
        super.setText(" AND ");
    }

    public TAndComparator(int line, int pos)
    {
        super.setText(" AND ");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TAndComparator(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTAndComparator(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TAndComparator text.");
    }
}
