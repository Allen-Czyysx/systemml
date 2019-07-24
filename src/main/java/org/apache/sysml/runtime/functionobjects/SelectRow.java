package org.apache.sysml.runtime.functionobjects;

public class SelectRow extends ValueComparisonFunction {

	private static final long serialVersionUID = -5444900552418046584L;

	private static SelectRow singleObj = null;

	private SelectRow() {
	}

	public static SelectRow getGreaterThanEqualsBlockFnObject() {
		if (singleObj == null)
			singleObj = new SelectRow();
		return singleObj;
	}

	@Override
	public ValueFunction getBasicFunction() {
		return GreaterThanEquals.getGreaterThanEqualsFnObject();
	}

	@Override
	public double execute(double in1, double in2) {
		return (in1 >= in2 ? 1.0 : 0.0);
	}

	@Override
	public boolean compare(double in1, double in2) {
		return (in1 >= in2);
	}

	@Override
	public boolean compare(long in1, long in2) {
		return (in1 >= in2);
	}

	@Override
	public boolean compare(boolean in1, boolean in2) {
		return (in1 && !in2) || (in1 == in2);
	}

	@Override
	public boolean compare(String in1, String in2) {
		return (in1 != null && in1.compareTo(in2) >= 0);
	}

}
