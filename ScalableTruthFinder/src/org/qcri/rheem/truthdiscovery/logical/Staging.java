package org.qcri.rheem.truthdiscovery.logical;

import org.qcri.rheem.core.data.RheemContext;
import org.qcri.rheem.core.logicallayer.LogicalOperator;

/**
 * Created by zoi on 22/1/15.
 */
public abstract class Staging<R, V> extends LogicalOperator {

    public abstract R staging (V input, RheemContext context);
}
