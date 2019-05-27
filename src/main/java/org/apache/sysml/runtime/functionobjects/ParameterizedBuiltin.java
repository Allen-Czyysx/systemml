/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.functionobjects;

import java.util.HashMap;

import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.AbstractIntegerDistribution;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.FDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.TDistribution;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.util.UtilFunctions;

/**
 * Function object for builtin function that takes a list of name=value
 * parameters. This class can not be instantiated elsewhere.
 */

public class ParameterizedBuiltin extends ValueFunction {

	private static final long serialVersionUID = -5966242955816522697L;

	public enum ParameterizedBuiltinCode {
		CDF, INVCDF, RMEMPTY, REPLACE, REXPAND, LOWER_TRI, UPPER_TRI, TRANSFORMAPPLY, TRANSFORMDECODE, PARAMSERV,
		REPARTITION,
	}

	public enum ProbabilityDistributionCode {
		INVALID, NORMAL, EXP, CHISQ, F, T, BINOMIAL
	}

	public ParameterizedBuiltinCode bFunc;
	public ProbabilityDistributionCode distFunc;

	static public HashMap<String, ParameterizedBuiltinCode> String2ParameterizedBuiltinCode;
	static {
		String2ParameterizedBuiltinCode = new HashMap<>();
		String2ParameterizedBuiltinCode.put("cdf", ParameterizedBuiltinCode.CDF);
		String2ParameterizedBuiltinCode.put("invcdf", ParameterizedBuiltinCode.INVCDF);
		String2ParameterizedBuiltinCode.put("rmempty", ParameterizedBuiltinCode.RMEMPTY);
		String2ParameterizedBuiltinCode.put("repartitionNonZeros", ParameterizedBuiltinCode.REPARTITION);
		String2ParameterizedBuiltinCode.put("replace", ParameterizedBuiltinCode.REPLACE);
		String2ParameterizedBuiltinCode.put("lowertri", ParameterizedBuiltinCode.LOWER_TRI);
		String2ParameterizedBuiltinCode.put("uppertri", ParameterizedBuiltinCode.UPPER_TRI);
		String2ParameterizedBuiltinCode.put("rexpand", ParameterizedBuiltinCode.REXPAND);
		String2ParameterizedBuiltinCode.put("transformapply", ParameterizedBuiltinCode.TRANSFORMAPPLY);
		String2ParameterizedBuiltinCode.put("transformdecode", ParameterizedBuiltinCode.TRANSFORMDECODE);
		String2ParameterizedBuiltinCode.put("paramserv", ParameterizedBuiltinCode.PARAMSERV);
	}

	static public HashMap<String, ProbabilityDistributionCode> String2DistCode;
	static {
		String2DistCode = new HashMap<>();
		String2DistCode.put("normal", ProbabilityDistributionCode.NORMAL);
		String2DistCode.put("exp", ProbabilityDistributionCode.EXP);
		String2DistCode.put("chisq", ProbabilityDistributionCode.CHISQ);
		String2DistCode.put("f", ProbabilityDistributionCode.F);
		String2DistCode.put("t", ProbabilityDistributionCode.T);
		String2DistCode.put("binomial", ProbabilityDistributionCode.BINOMIAL);
	}

	// We should create one object for every builtin function that we support
	private static ParameterizedBuiltin normalObj = null, expObj = null, chisqObj = null, fObj = null, tObj = null,
			binomialObj = null;
	private static ParameterizedBuiltin inormalObj = null, iexpObj = null, ichisqObj = null, ifObj = null, itObj = null, ibinomialObj;

	private ParameterizedBuiltin(ParameterizedBuiltinCode bf) {
		bFunc = bf;
		distFunc = ProbabilityDistributionCode.INVALID;
	}

	private ParameterizedBuiltin(ParameterizedBuiltinCode bf, ProbabilityDistributionCode dist) {
		bFunc = bf;
		distFunc = dist;
	}

	public static ParameterizedBuiltin getParameterizedBuiltinFnObject(String str) {
		return getParameterizedBuiltinFnObject(str, null);
	}

	public static ParameterizedBuiltin getParameterizedBuiltinFnObject(String str, String str2) {

		ParameterizedBuiltinCode code = String2ParameterizedBuiltinCode.get(str);

		switch (code) {
			case CDF:
				// str2 will point the appropriate distribution
				ProbabilityDistributionCode dcode = String2DistCode.get(str2.toLowerCase());

				switch (dcode) {
					case NORMAL:
						if (normalObj == null)
							normalObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.CDF, dcode);
						return normalObj;
					case EXP:
						if (expObj == null)
							expObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.CDF, dcode);
						return expObj;
					case CHISQ:
						if (chisqObj == null)
							chisqObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.CDF, dcode);
						return chisqObj;
					case F:
						if (fObj == null)
							fObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.CDF, dcode);
						return fObj;
					case T:
						if (tObj == null)
							tObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.CDF, dcode);
						return tObj;
					case BINOMIAL:
						if (binomialObj == null)
							binomialObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.CDF, dcode);
						return binomialObj;
					default:
						throw new DMLRuntimeException("Invalid distribution code: " + dcode);
				}

			case INVCDF:
				// str2 will point the appropriate distribution
				ProbabilityDistributionCode distcode = String2DistCode.get(str2.toLowerCase());

				switch (distcode) {
					case NORMAL:
						if (inormalObj == null)
							inormalObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.INVCDF, distcode);
						return inormalObj;
					case EXP:
						if (iexpObj == null)
							iexpObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.INVCDF, distcode);
						return iexpObj;
					case CHISQ:
						if (ichisqObj == null)
							ichisqObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.INVCDF, distcode);
						return ichisqObj;
					case F:
						if (ifObj == null)
							ifObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.INVCDF, distcode);
						return ifObj;
					case T:
						if (itObj == null)
							itObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.INVCDF, distcode);
						return itObj;
					case BINOMIAL:
						if (ibinomialObj == null)
							ibinomialObj = new ParameterizedBuiltin(ParameterizedBuiltinCode.INVCDF, distcode);
						return ibinomialObj;
					default:
						throw new DMLRuntimeException("Invalid distribution code: " + distcode);
				}

			case RMEMPTY:
			case REPARTITION:
			case REPLACE:
			case LOWER_TRI:
			case UPPER_TRI:
			case REXPAND:
			case TRANSFORMAPPLY:
			case TRANSFORMDECODE:
			case PARAMSERV:
				return new ParameterizedBuiltin(code);

			default:
				throw new DMLRuntimeException("Invalid parameterized builtin code: " + code);
		}
	}

	@Override
	public double execute(HashMap<String, String> params) {
		switch (bFunc) {
		case CDF:
		case INVCDF:
			switch (distFunc) {
			case NORMAL:
			case EXP:
			case CHISQ:
			case F:
			case T:
			case BINOMIAL:
				return computeFromDistribution(distFunc, params, (bFunc == ParameterizedBuiltinCode.INVCDF));
			default:
				throw new DMLRuntimeException("Unsupported distribution (" + distFunc + ").");
			}

		default:
			throw new DMLRuntimeException("ParameterizedBuiltin.execute(): Unknown operation: " + bFunc);
		}
	}

	/**
	 * Helper function to compute distribution-specific cdf (both lowertail and
	 * uppertail) and inverse cdf.
	 * 
	 * @param dcode   probablility distribution code
	 * @param params  map of parameters
	 * @param inverse true if inverse
	 * @return cdf or inverse cdf
	 */
	private static double computeFromDistribution(ProbabilityDistributionCode dcode, HashMap<String, String> params,
			boolean inverse) {

		// given value is "quantile" when inverse=false, and it is "probability" when
		// inverse=true
		double val = Double.parseDouble(params.get("target"));

		boolean lowertail = true;
		if (params.get("lower.tail") != null) {
			lowertail = Boolean.parseBoolean(params.get("lower.tail"));
		}

		AbstractRealDistribution distFunction = null;
		AbstractIntegerDistribution distIntegerFunction = null;

		switch (dcode) {
		case NORMAL:

			double mean = 0.0, sd = 1.0; // default values for mean and sd

			String mean_s = params.get("mean"), sd_s = params.get("sd");
			if (mean_s != null)
				mean = Double.parseDouble(mean_s);
			if (sd_s != null)
				sd = Double.parseDouble(sd_s);

			if (sd <= 0)
				throw new DMLRuntimeException(
						"Standard deviation for Normal distribution must be positive (" + sd + ")");

			distFunction = new NormalDistribution(mean, sd);
			break;

		case EXP:
			double exp_rate = 1.0; // default value for 1/mean or rate

			if (params.get("rate") != null)
				exp_rate = Double.parseDouble(params.get("rate"));
			if (exp_rate <= 0) {
				throw new DMLRuntimeException("Rate for Exponential distribution must be positive (" + exp_rate + ")");
			}
			// For exponential distribution: mean = 1/rate
			distFunction = new ExponentialDistribution(1.0 / exp_rate);
			break;

		case CHISQ:
			if (params.get("df") == null) {
				throw new DMLRuntimeException("" + "Degrees of freedom must be specified for chi-squared distribution "
						+ "(e.g., q=qchisq(0.5, df=20); p=pchisq(target=q, df=1.2))");
			}
			int df = UtilFunctions.parseToInt(params.get("df"));

			if (df <= 0) {
				throw new DMLRuntimeException(
						"Degrees of Freedom for chi-squared distribution must be positive (" + df + ")");
			}
			distFunction = new ChiSquaredDistribution(df);
			break;

		case F:
			if (params.get("df1") == null || params.get("df2") == null) {
				throw new DMLRuntimeException("" + "Degrees of freedom must be specified for F distribution "
						+ "(e.g., q = qf(target=0.5, df1=20, df2=30); p=pf(target=q, df1=20, df2=30))");
			}
			int df1 = UtilFunctions.parseToInt(params.get("df1"));
			int df2 = UtilFunctions.parseToInt(params.get("df2"));
			if (df1 <= 0 || df2 <= 0) {
				throw new DMLRuntimeException(
						"Degrees of Freedom for F distribution must be positive (" + df1 + "," + df2 + ")");
			}
			distFunction = new FDistribution(df1, df2);
			break;

		case T:
			if (params.get("df") == null) {
				throw new DMLRuntimeException(
						"" + "Degrees of freedom is needed to compute probabilities from t distribution "
								+ "(e.g., q = qt(target=0.5, df=10); p = pt(target=q, df=10))");
			}
			int t_df = UtilFunctions.parseToInt(params.get("df"));
			if (t_df <= 0) {
				throw new DMLRuntimeException("Degrees of Freedom for t distribution must be positive (" + t_df + ")");
			}
			distFunction = new TDistribution(t_df);
			break;

		case BINOMIAL:
			try {
				if (!inverse)
					Integer.parseInt(params.get("target"));
			} catch (NumberFormatException e) {
				throw new DMLRuntimeException(
						"" + "Target needs to be an integer " + "(e.g., p=pbinomial(target=1, trials=10, p=0.3))("+val+")");
			}

			int trials;
			if (params.get("trials") == null) {
				throw new DMLRuntimeException("" + "Number of trials must be specified for binomial distribution "
						+ "(e.g., p=pbinomial(target=q, trials=10))");
			}

			try {
				trials = Integer.parseInt(params.get("trials"));
			} catch (NumberFormatException e) {
				throw new DMLRuntimeException(
						"" + "trials needs to be an integer " + "(e.g., p=pbinomial(target=1, trials=10, p=0.3)");
			}

			if (trials < 0) {
				throw new DMLRuntimeException(
						"Number of trials must be positive (NotPositiveException - if trials < 0) (" + trials + ")");
			}

			if (params.get("p") == null) {
				throw new DMLRuntimeException("" + "Probability of success must be specified for binomial distribution "
						+ "(e.g., p=pbinomial(target=1, trials=10, p=0.3))");
			}

			double p = UtilFunctions.parseToDouble(params.get("p"));
			if (p < 0 || p > 1) {
				throw new DMLRuntimeException(
						"" + "Probability of success must be 0<=p<=1 (OutOfRangeException) (" + p + ")");
			}

			distIntegerFunction = new BinomialDistribution(trials, p);
			break;

		default:
			throw new DMLRuntimeException("Invalid distribution code: " + dcode);

		}

		double ret = Double.NaN;
		if (inverse) {
			// inverse cdf
			ret = (distIntegerFunction == null) ? distFunction.inverseCumulativeProbability(val)
					: distIntegerFunction.inverseCumulativeProbability(val);
		} else if (lowertail) {
			// cdf (lowertail)
			ret = (distIntegerFunction == null) ? distFunction.cumulativeProbability(val)
					: distIntegerFunction.cumulativeProbability((int) val);
		} else {
			// cdf (upper tail)

			// TODO: more accurate distribution-specific computation of upper tail
			// probabilities
			ret = 1.0 - ((distIntegerFunction == null) ? distFunction.cumulativeProbability(val)
					: distIntegerFunction.cumulativeProbability((int) val));
		}

		return ret;
	}
}
