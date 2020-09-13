/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.Analysis;

@SuppressWarnings("nls")
public final class ARegTermTerm extends PTerm
{
    private PRegTerm _regTerm_;

    public ARegTermTerm()
    {
        // Constructor
    }

    public ARegTermTerm(
        @SuppressWarnings("hiding") PRegTerm _regTerm_)
    {
        // Constructor
        setRegTerm(_regTerm_);

    }

    @Override
    public Object clone()
    {
        return new ARegTermTerm(
            cloneNode(this._regTerm_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseARegTermTerm(this);
    }

    public PRegTerm getRegTerm()
    {
        return this._regTerm_;
    }

    public void setRegTerm(PRegTerm node)
    {
        if(this._regTerm_ != null)
        {
            this._regTerm_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._regTerm_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._regTerm_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._regTerm_ == child)
        {
            this._regTerm_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._regTerm_ == oldChild)
        {
            setRegTerm((PRegTerm) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
