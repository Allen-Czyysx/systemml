package org.apache.sysml.runtime.functionobjects;

import java.io.Serializable;

public class MultiplyBlock extends ValueFunction implements Serializable {

	private static final long serialVersionUID = 2801982061205871665L;

	private static MultiplyBlock singleObj = null;

	private MultiplyBlock() {
	}

	public static MultiplyBlock getMultiplyBlockFnObject() {
		if (singleObj == null)
			singleObj = new MultiplyBlock();
		return singleObj;
	}

	@Override
	public ValueFunction getBasicFunction() {
		return Multiply.getMultiplyFnObject();
	}

	@Override
	public double execute(double in1, double in2) {
		return in1 * in2;
	}

	@Override
	public double execute(long in1, long in2) {
		double dval = ((double) in1 * in2);
		if (dval > Long.MAX_VALUE) {
			return dval;
		}
		return in1 * in2;
	}

}
