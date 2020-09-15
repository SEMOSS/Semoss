/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AWordOrIdScalar extends PScalar
{
    private PWordOrId _wordOrId_;

    public AWordOrIdScalar()
    {
        // Constructor
    }

    public AWordOrIdScalar(
        @SuppressWarnings("hiding") PWordOrId _wordOrId_)
    {
        // Constructor
        setWordOrId(_wordOrId_);

    }

    @Override
    public Object clone()
    {
        return new AWordOrIdScalar(
            cloneNode(this._wordOrId_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAWordOrIdScalar(this);
    }

    public PWordOrId getWordOrId()
    {
        return this._wordOrId_;
    }

    public void setWordOrId(PWordOrId node)
    {
        if(this._wordOrId_ != null)
        {
            this._wordOrId_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._wordOrId_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._wordOrId_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._wordOrId_ == child)
        {
            this._wordOrId_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._wordOrId_ == oldChild)
        {
            setWordOrId((PWordOrId) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
