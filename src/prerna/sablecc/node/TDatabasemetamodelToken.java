/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class TDatabasemetamodelToken extends Token
{
    public TDatabasemetamodelToken()
    {
        super.setText("database.metamodel");
    }

    public TDatabasemetamodelToken(int line, int pos)
    {
        super.setText("database.metamodel");
        setLine(line);
        setPos(pos);
    }

    @Override
    public Object clone()
    {
      return new TDatabasemetamodelToken(getLine(), getPos());
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseTDatabasemetamodelToken(this);
    }

    @Override
    public void setText(@SuppressWarnings("unused") String text)
    {
        throw new RuntimeException("Cannot change TDatabasemetamodelToken text.");
    }
}
