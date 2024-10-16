/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AHelpExpr extends PExpr
{
    private TId _id_;
    private THelpToken _helpToken_;

    public AHelpExpr()
    {
        // Constructor
    }

    public AHelpExpr(
        @SuppressWarnings("hiding") TId _id_,
        @SuppressWarnings("hiding") THelpToken _helpToken_)
    {
        // Constructor
        setId(_id_);

        setHelpToken(_helpToken_);

    }

    @Override
    public Object clone()
    {
        return new AHelpExpr(
            cloneNode(this._id_),
            cloneNode(this._helpToken_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAHelpExpr(this);
    }

    public TId getId()
    {
        return this._id_;
    }

    public void setId(TId node)
    {
        if(this._id_ != null)
        {
            this._id_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._id_ = node;
    }

    public THelpToken getHelpToken()
    {
        return this._helpToken_;
    }

    public void setHelpToken(THelpToken node)
    {
        if(this._helpToken_ != null)
        {
            this._helpToken_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._helpToken_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._id_)
            + toString(this._helpToken_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._id_ == child)
        {
            this._id_ = null;
            return;
        }

        if(this._helpToken_ == child)
        {
            this._helpToken_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._id_ == oldChild)
        {
            setId((TId) newChild);
            return;
        }

        if(this._helpToken_ == oldChild)
        {
            setHelpToken((THelpToken) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
