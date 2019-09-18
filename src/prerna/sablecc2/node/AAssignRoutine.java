/* This file was generated by SableCC (http://www.sablecc.org/). */

package prerna.sablecc2.node;

import java.util.*;
import prerna.sablecc2.analysis.*;

@SuppressWarnings("nls")
public final class AAssignRoutine extends PRoutine
{
    private PAssignment _assignment_;
    private final LinkedList<TSemicolon> _semicolon_ = new LinkedList<TSemicolon>();

    public AAssignRoutine()
    {
        // Constructor
    }

    public AAssignRoutine(
        @SuppressWarnings("hiding") PAssignment _assignment_,
        @SuppressWarnings("hiding") List<?> _semicolon_)
    {
        // Constructor
        setAssignment(_assignment_);

        setSemicolon(_semicolon_);

    }

    @Override
    public Object clone()
    {
        return new AAssignRoutine(
            cloneNode(this._assignment_),
            cloneList(this._semicolon_));
    }

    @Override
    public void apply(Switch sw)
    {
        ((Analysis) sw).caseAAssignRoutine(this);
    }

    public PAssignment getAssignment()
    {
        return this._assignment_;
    }

    public void setAssignment(PAssignment node)
    {
        if(this._assignment_ != null)
        {
            this._assignment_.parent(null);
        }

        if(node != null)
        {
            if(node.parent() != null)
            {
                node.parent().removeChild(node);
            }

            node.parent(this);
        }

        this._assignment_ = node;
    }

    public LinkedList<TSemicolon> getSemicolon()
    {
        return this._semicolon_;
    }

    public void setSemicolon(List<?> list)
    {
        for(TSemicolon e : this._semicolon_)
        {
            e.parent(null);
        }
        this._semicolon_.clear();

        for(Object obj_e : list)
        {
            TSemicolon e = (TSemicolon) obj_e;
            if(e.parent() != null)
            {
                e.parent().removeChild(e);
            }

            e.parent(this);
            this._semicolon_.add(e);
        }
    }

    @Override
    public String toString()
    {
        return ""
            + toString(this._assignment_)
            + toString(this._semicolon_);
    }

    @Override
    void removeChild(@SuppressWarnings("unused") Node child)
    {
        // Remove child
        if(this._assignment_ == child)
        {
            this._assignment_ = null;
            return;
        }

        if(this._semicolon_.remove(child))
        {
            return;
        }

        throw new RuntimeException("Not a child.");
    }

    @Override
    void replaceChild(@SuppressWarnings("unused") Node oldChild, @SuppressWarnings("unused") Node newChild)
    {
        // Replace child
        if(this._assignment_ == oldChild)
        {
            setAssignment((PAssignment) newChild);
            return;
        }

        for(ListIterator<TSemicolon> i = this._semicolon_.listIterator(); i.hasNext();)
        {
            if(i.next() == oldChild)
            {
                if(newChild != null)
                {
                    i.set((TSemicolon) newChild);
                    newChild.parent(this);
                    oldChild.parent(null);
                    return;
                }

                i.remove();
                oldChild.parent(null);
                return;
            }
        }

        throw new RuntimeException("Not a child.");
    }
}
