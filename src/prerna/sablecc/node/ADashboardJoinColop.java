/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class ADashboardJoinColop extends PColop
{
    private PDashboardJoin _dashboardJoin_;

    public ADashboardJoinColop()
    {
        // Constructor
    }

    public ADashboardJoinColop(
        @SuppressWarnings("hiding") PDashboardJoin _dashboardJoin_)
    {
        // Constructor
        setDashboardJoin(_dashboardJoin_);

    }

    @Override
    public Object clone()
    {
        return new ADashboardJoinColop(
            cloneNode(this._dashboardJoin_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseADashboardJoinColop(this);
    }

    public PDashboardJoin getDashboardJoin()
    {
        return this._dashboardJoin_;
    }

    public void setDashboardJoin(PDashboardJoin node)
    {
        if(this._dashboardJoin_ != null)
        {
            this._dashboardJoin_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._dashboardJoin_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._dashboardJoin_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._dashboardJoin_ == child)
        {
            this._dashboardJoin_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._dashboardJoin_ == oldChild)
        {
            setDashboardJoin((PDashboardJoin) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
