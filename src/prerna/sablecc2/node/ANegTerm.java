/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.Analysis;

@SuppressWarnings("nls")
public final class ANegTerm extends PNegTerm
{
    private TMinus _minus_;
    private PTerm _term_;

    public ANegTerm()
    {
        // Constructor
    }

    public ANegTerm(
        @SuppressWarnings("hiding") TMinus _minus_,
        @SuppressWarnings("hiding") PTerm _term_)
    {
        // Constructor
        setMinus(_minus_);

        setTerm(_term_);

    }

    @Override
    public Object clone()
    {
        return new ANegTerm(
            cloneNode(this._minus_),
            cloneNode(this._term_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseANegTerm(this);
    }

    public TMinus getMinus()
    {
        return this._minus_;
    }

    public void setMinus(TMinus node)
    {
        if(this._minus_ != null)
        {
            this._minus_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._minus_ = node;
    }

    public PTerm getTerm()
    {
        return this._term_;
    }

    public void setTerm(PTerm node)
    {
        if(this._term_ != null)
        {
            this._term_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._term_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._minus_)
            + toString(this._term_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._minus_ == child)
        {
            this._minus_ = null;
            return;
        }

        if(this._term_ == child)
        {
            this._term_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._minus_ == oldChild)
        {
            setMinus((TMinus) newChild);
            return;
        }

        if(this._term_ == oldChild)
        {
            setTerm((PTerm) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
