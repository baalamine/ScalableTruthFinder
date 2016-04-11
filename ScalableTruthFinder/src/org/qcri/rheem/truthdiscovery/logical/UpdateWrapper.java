package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperatorWrapper;

/**
 * Created by zoi on 1/2/15.
 */
public class UpdateWrapper<R> extends LogicalOperatorWrapper<R> {

    Update logOp;

    public UpdateWrapper(Update logOp) {
        this.logOp = logOp;
    }

    @Override
    public R apply(RheemContext context, Object... args) {
        return (R) this.logOp.update(args[0], context);
    }

}
