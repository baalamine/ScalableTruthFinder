package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

/**
 * Created by zoi on 25/1/15.
 */
public class ComputeWrapper<R> extends LogicalOperatorWrapper {

    Compute logOp;

    public ComputeWrapper(Compute logOp) {
        this.logOp = logOp;
    }

    @Override
    public R apply(RheemContext context, Object... args) {
        return (R) this.logOp.compute(args[0], context);
    }

    @Override
    public void initialise() { logOp.initialise(); }
}
