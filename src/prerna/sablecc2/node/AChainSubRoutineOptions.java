/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AChainSubRoutineOptions extends PSubRoutineOptions
{
    private PBaseSubScript _baseSubScript_;

    public AChainSubRoutineOptions()
    {
        // Constructor
    }

    public AChainSubRoutineOptions(
        @SuppressWarnings("hiding") PBaseSubScript _baseSubScript_)
    {
        // Constructor
        setBaseSubScript(_baseSubScript_);

    }

    @Override
    public Object clone()
    {
        return new AChainSubRoutineOptions(
            cloneNode(this._baseSubScript_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAChainSubRoutineOptions(this);
    }

    public PBaseSubScript getBaseSubScript()
    {
        return this._baseSubScript_;
    }

    public void setBaseSubScript(PBaseSubScript node)
    {
        if(this._baseSubScript_ != null)
        {
            this._baseSubScript_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._baseSubScript_ = node;
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._baseSubScript_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._baseSubScript_ == child)
        {
            this._baseSubScript_ = null;
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._baseSubScript_ == oldChild)
        {
            setBaseSubScript((PBaseSubScript) newChild);
            return;
        }

        throw new RuntimeException("Not a child.");
    }
}
