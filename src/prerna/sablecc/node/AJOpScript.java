/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc.node;

import prerna.sablecc.analysis.*;

@SuppressWarnings("nls")
public final class AJOpScript extends PScript
{
    private PJOp _jOp_;
    private TSemicolon _semicolon_;

    public AJOpScript()
    {
        // Constructor
    }

    public AJOpScript(
        @SuppressWarnings("hiding") PJOp _jOp_,
        @SuppressWarnings("hiding") TSemicolon _semicolon_)
    {
        // Constructor
        setJOp(_jOp_);

        setSemicolon(_semicolon_);

    }

    @Override
    public Object clone()
    {
        return new AJOpScript(
            cloneNode(this._jOp_),
            cloneNode(this._semicolon_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAJOpScript(this);
    }

    public PJOp getJOp()
    {
        return this._jOp_;
    }

    public void setJOp(PJOp node)
    {
        if(this._jOp_ != null)
        {
            this._jOp_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._jOp_ = node;
    }

    public TSemicolon getSemicolon()
    {
        return this._semicolon_;
    }

    public void setSemicolon(TSemicolon node)
    {
        if(this._semicolon_ != null)
        {
            this._semicolon_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._semicolon_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._jOp_)
            + toString(this._semicolon_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._jOp_ == child)
        {
            this._jOp_ = null;
            return;
        }

        if(this._semicolon_ == child)
        {
            this._semicolon_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._jOp_ == oldChild)
        {
            setJOp((PJOp) newChild);
            return;
        }

        if(this._semicolon_ == oldChild)
        {
            setSemicolon((TSemicolon) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
