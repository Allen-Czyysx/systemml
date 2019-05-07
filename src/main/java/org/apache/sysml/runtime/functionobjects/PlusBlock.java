package org.apache.sysml.runtime.functionobjects;

import java.io.Serializable;

public class PlusBlock extends ValueFunction implements Serializable {
	private static final long serialVersionUID = -3573790367761963555L;

	private static PlusBlock singleObj = null;

	private PlusBlock() {
	}

	public static PlusBlock getPlusBlockFnObject() {
		if (singleObj == null)
			singleObj = new PlusBlock();
		return singleObj;
	}

	@Override
	public double execute(double in1, double in2) {
		return in1 + in2;
	}

	@Override
	public double execute(long in1, long in2) {
		double dval = ((double) in1 + in2);
		if (dval > Long.MAX_VALUE)
			return dval;

		return in1 + in2;
	}

	@Override
	public String execute(String in1, String in2) {
		return in1 + in2;
	}

}
