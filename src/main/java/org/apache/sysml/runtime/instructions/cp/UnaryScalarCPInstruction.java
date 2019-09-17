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

package org.apache.sysml.runtime.instructions.cp;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.*;
import org.apache.sysml.hops.codegen.SpoofCompiler;
import org.apache.sysml.hops.recompile.RecompileStatus;
import org.apache.sysml.lops.*;
import org.apache.sysml.parser.DWhileStatement;
import org.apache.sysml.parser.Expression.ValueType;
import org.apache.sysml.parser.IfStatementBlock;
import org.apache.sysml.parser.StatementBlock;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLScriptException;
import org.apache.sysml.runtime.controlprogram.DWhileProgramBlock;
import org.apache.sysml.runtime.controlprogram.IfProgramBlock;
import org.apache.sysml.runtime.controlprogram.LocalVariableMap;
import org.apache.sysml.runtime.controlprogram.ProgramBlock;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.spark.BinarySPInstruction;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;

import java.util.ArrayList;

import static org.apache.sysml.hops.OptimizerUtils.DEFAULT_BLOCKSIZE;
import static org.apache.sysml.hops.recompile.Recompiler.*;

public class UnaryScalarCPInstruction extends UnaryMatrixCPInstruction {

	protected UnaryScalarCPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String instr) {
		super(op, in, out, opcode, instr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		String opcode = getOpcode();
		ScalarObject sores = null;
		ScalarObject so = null;

		//get the scalar input 
		so = ec.getScalarInput(input1);

		//core execution
		if (DWhileStatement.isVarUseDeltaName(input1.getName())
				&& DWhileStatement.isVarUseDeltaName(input1.getName())
				&& opcode.equals("!")) {
			DWhileProgramBlock pb = DWhileProgramBlock._pb;
			if (pb == null) {
				throw new DMLRuntimeException("DWhileProgramBlock._pb shouldn't be null");
			}

			IfProgramBlock mainPb = (IfProgramBlock) (pb.getChildBlocks().get(0));
			ArrayList<Hop> newComHops = recompileProgramBlock(ec, mainPb.getChildBlocksElseBody());
			ArrayList<Hop> newIncHops = recompileProgramBlock(ec, mainPb.getChildBlocksIfBody());
			ArrayList<Hop> newAftHops = recompileProgramBlock(ec, pb.getDIterAfter());

			HopCost comCost = estimateCost(newComHops);
			HopCost incCost = estimateCost(newIncHops);
			HopCost aftCost = estimateCost(newAftHops);

//			System.out.println("com" + comCost);
//			System.out.println("inc" + incCost);
//			System.out.println("aft" + aftCost);

//			if (so.getBooleanValue()) {
			if (true) {
				System.out.println("use delta next time");
//				sores = new BooleanObject(so.getBooleanValue());
				sores = new BooleanObject(true);
			}

		} else if (opcode.equalsIgnoreCase("print")) {
			String outString = so.getLanguageSpecificStringValue();

			// print to stdout only when suppress flag in DMLScript is not set.
			// The flag will be set, for example, when SystemML is invoked in fenced mode from Jaql.
			if (!DMLScript.suppressPrint2Stdout())
				System.out.println(outString);

			// String that is printed on stdout will be inserted into symbol table (dummy, not necessary!) 
			sores = new StringObject(outString);

		} else if (opcode.equalsIgnoreCase("stop")) {
			throw new DMLScriptException(so.getStringValue());

		} else if (opcode.equalsIgnoreCase("assert")) {
			sores = new BooleanObject(so.getBooleanValue());
			if (!so.getBooleanValue()) {
				String fileName = this.getFilename() == null ? "" : this.getFilename() + " ";
				throw new DMLScriptException("assertion failed at " + fileName + this.getBeginLine() + ":"
						+ this.getBeginColumn() + "-" + this.getEndLine() + ":" + this.getEndColumn());
			}

		} else {
			UnaryOperator dop = (UnaryOperator) _optr;
			if (so instanceof IntObject && output.getValueType() == ValueType.INT)
				sores = new IntObject((long) dop.fn.execute(so.getLongValue()));
			else if (so instanceof BooleanObject && output.getValueType() == ValueType.BOOLEAN)
				sores = new BooleanObject(dop.fn.execute(so.getBooleanValue()));
			else
				sores = new DoubleObject(dop.fn.execute(so.getDoubleValue()));
		}

		ec.setScalarOutput(output.getName(), sores);
	}

	private ArrayList<Hop> recompileHops(ExecutionContext ec, ArrayList<Hop> hops, boolean recompile) {
		ArrayList<Hop> ret;

		try {
			if (ConfigurationManager.isDynamicRecompilation() && recompile) {
				LocalVariableMap vars = ec.getVariables();
				RecompileStatus status = null;
				boolean forceEt = false;
				LopProperties.ExecType et = null;
				boolean codegen = ConfigurationManager.isCodegenEnabled()
						&& !(forceEt && et == null)
						&& SpoofCompiler.RECOMPILE_CODEGEN;

				// prepare hops dag for recompile
				hops = deepCopyHopsDag(hops);

				// get max parallelism constraint, see below
				Hop.resetVisitStatus(hops);
				int maxK = rGetMaxParallelism(hops);

				// replace scalar reads with literals
				Hop.resetVisitStatus(hops);
				for (Hop hopRoot : hops) {
					rReplaceLiterals(hopRoot, vars, false);
				}

				// update statistics, rewrites, and mem estimates
				// refresh matrix characteristics (update stats)
				Hop.resetVisitStatus(hops);
				for (Hop hopRoot : hops) {
					rUpdateStatistics(hopRoot, vars);
				}

				// dynamic hop rewrites
				_rewriter.get().rewriteHopDAG(hops, null);

				//update stats after rewrites
				Hop.resetVisitStatus(hops);
				for (Hop hopRoot : hops) {
					rUpdateStatistics(hopRoot, vars);
				}

				// refresh memory estimates (based on updated stats,
				// before: init memo table with propagated worst-case estimates,
				// after: extract worst-case estimates from memo table
				Hop.resetVisitStatus(hops);
				MemoTable memo = new MemoTable();
				memo.init(hops, status);
				Hop.resetVisitStatus(hops);
				for (Hop hopRoot : hops)
					hopRoot.refreshMemEstimates(memo);
				memo.extract(hops, status);

				// codegen if enabled
				if (codegen) {
					Hop.resetVisitStatus(hops);
					hops = SpoofCompiler.optimize(hops, true);
				}

				// set max parallelism constraint to ensure compilation
				// incl rewrites does not lose these hop-lop constraints
				Hop.resetVisitStatus(hops);
				rSetMaxParallelism(hops, maxK);

				// construct lops
				for (Hop hopRoot : hops) {
					hopRoot.constructLops();
				}

				ret = hops;

			} else {
				ret = hops;
			}

		} catch (Exception ex) {
			throw new DMLRuntimeException("Unable to recompile program block.", ex);
		}

		return ret;
	}

	private ArrayList<Hop> recompileProgramBlock(ExecutionContext ec, ArrayList<ProgramBlock> pbs) {
		ArrayList<Hop> ret = new ArrayList<>();

		for (ProgramBlock pb : pbs) {
			StatementBlock sb = pb.getStatementBlock();

			if (pb instanceof IfProgramBlock) {
				IfProgramBlock ipb = (IfProgramBlock) pb;
				IfStatementBlock isb = (IfStatementBlock) sb;

				ArrayList<Hop> predicate = new ArrayList<>(1);
				predicate.add(isb.getPredicateHops());
				ret.addAll(recompileHops(ec, predicate, isb.requiresPredicateRecompilation()));

				ret.addAll(recompileProgramBlock(ec, ipb.getChildBlocksIfBody()));

				ret.addAll(recompileProgramBlock(ec, ipb.getChildBlocksIfBody()));

			} else if (pb.getInstructions().size() != 0) {
				ret.addAll(recompileHops(ec, sb.getHops(), sb.requiresRecompilation()));

			} else {
				throw new DMLRuntimeException("unsupport program block");
			}
		}

		return ret;
	}

	private HopCost estimateCost(Hop hop) {
		HopCost cost = new HopCost();
		Lop lop = hop.getLops();
//		if (lop == null && hop instanceof ReorgOp && ((ReorgOp) hop).getOp() == Hop.ReOrgOp.TRANS) {
		if (lop == null) {
			for (Hop child : hop.getInput()) {
				HopCost childCost = estimateCost(child);
				cost.add(childCost);
			}
			return cost;
		}
		LopProperties lps = lop.lps;

		// 已访问过
		if (hop.isEstimated()) {
			if (lps.getExecType() == LopProperties.ExecType.SPARK
					&& (lop instanceof MMCJ
					|| lop instanceof MapMult
					|| lop instanceof Binary)) { // TODO added by czh
				cost.isRDD = hop.isRDD();
			}
			return cost;
		}
		hop.setIsEstimated(true);

		// 估计子 hop 的代价
		int inLength = hop.getInput().size();
		boolean[] isRDD = new boolean[inLength];
		for (int i = 0; i < inLength; i++) {
			HopCost childCost = estimateCost(hop.getInput().get(i));
			cost.add(childCost);
			isRDD[i] = childCost.isRDD;
		}

		if (lop instanceof MMCJ || lop instanceof MapMult) { // CPMM, MAPMM
			Hop in1 = hop.getInput().get(0);
			Hop in2 = hop.getInput().get(1);
			long in1r = in1.getDim1();
			long in1c = in1.getDim2();
			long in2r = in2.getDim1();
			long in2c = in2.getDim2();
			double in1s = OptimizerUtils.getSparsity(in1r, in1c, in1.getNnz());
			double in2s = OptimizerUtils.getSparsity(in2r, in2c, in2.getNnz());

			ExecutionContext ec = DWhileProgramBlock._ec;
			boolean useDelta = hop.needCache() && DWhileStatement.isDWhileTmpVar(in2.getName()) && ec != null;
			boolean useRepartition = useDelta &&
					(BinarySPInstruction.needRepartition(in2.getName())
							|| BinarySPInstruction.hasRepartitioned(in1.getName(), in2.getName()));

			if (useDelta) {
				String dVarName = DWhileStatement.getDVarNameFromTmpVar(in2.getName());
				String selectNumName = DWhileStatement.getSelectNumName(dVarName);
				long selectNum = ec.getScalarInput(selectNumName, ValueType.INT, false).getLongValue();
				double shrink = 1.0 * selectNum / in1c;
				in2s *= shrink;

				if (useRepartition) {
					String blockNumName = DWhileStatement.getRepartitionBlockNumName(dVarName);
					double blockNum = ec.getScalarInput(blockNumName, ValueType.INT, false).getDoubleValue();
					in1s *= blockNum / Math.ceil(1.0 * in2r / DEFAULT_BLOCKSIZE);
				}
			}

			double interS = 1 - Math.pow(1 - in1s * in2s, DEFAULT_BLOCKSIZE);
			if (useRepartition) {
				String dVarName = DWhileStatement.getDVarNameFromTmpVar(in2.getName());
				String blockNumName = DWhileStatement.getRepartitionBlockNumName(dVarName);
				double blockNum = ec.getScalarInput(blockNumName, ValueType.INT, false).getDoubleValue();
				interS *= blockNum / Math.ceil(1.0 * in2r / DEFAULT_BLOCKSIZE);
			}

			long m1SizeP = OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in1r, in1c, in1.getRowsInBlock(), in1.getColsInBlock(), in1s);
			long m2SizeP = OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in2r, in2c, in2.getRowsInBlock(), in2.getColsInBlock(), in2s);
			long mInterSizeP = OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in1r, in1c, in1.getRowsInBlock(), in1.getColsInBlock(), interS);

			if (lop instanceof MMCJ) { // CPMM
				cost.join += m1SizeP + m2SizeP;

				if (((MMCJ) lop)._aggtype == AggBinaryOp.SparkAggType.SINGLE_BLOCK) {
					cost.collect += mInterSizeP;
				} else {
					cost.shuffle += mInterSizeP;
				}

				cost.isRDD = true;

			} else { // MAPMM
				if (isRDD[1]) {
					if (!OptimizerUtils.checkSparkCollectMemoryBudget(in2r, in2c, in1.getRowsInBlock(),
							in1.getColsInBlock(), in1.getNnz(), 0, true)) {
						cost.hdfs += m2SizeP;
					} else {
						cost.collect += m2SizeP;
					}
				}

				cost.broadcast += m2SizeP;

				if (((MapMult) lop)._aggtype == AggBinaryOp.SparkAggType.SINGLE_BLOCK) {
					cost.collect += mInterSizeP;
				} else {
					cost.shuffle += mInterSizeP;
				}

				cost.isRDD = true;
			}

		} else if (lop instanceof MMTSJ) { // TSMM
			MMTSJ tsmm = (MMTSJ) lop;
			Hop in = hop.getInput().get(0);
			long inR = in.getDim1();
			long inC = in.getDim2();
			double inS = OptimizerUtils.getSparsity(inR, inC, in.getNnz());

			if (tsmm._multiPass) {
				throw new DMLRuntimeException("Cannot estimate tsmm2");
			}

			cost.collect += OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in.getDim1(), in.getDim2(), in.getRowsInBlock(), in.getColsInBlock(), inS * inS);

		} else if (lps.getExecType() == LopProperties.ExecType.SPARK && lop instanceof Binary) { // binary
			// matrix-matrix
			Hop in1 = hop.getInput().get(0);
			Hop in2 = hop.getInput().get(1);

			long m1SizeP = OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in1.getDim1(), in1.getDim2(), in1.getRowsInBlock(), in1.getColsInBlock(), in1.getNnz());
			long m2SizeP = OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in2.getDim1(), in2.getDim2(), in2.getRowsInBlock(), in2.getColsInBlock(), in2.getNnz());
			cost.join += m1SizeP + m2SizeP;

			cost.isRDD = true;

		} else if (lps.getExecType() == LopProperties.ExecType.SPARK && lop instanceof Unary) { // binary matrix-scalar
			cost.isRDD = true;

		} else if (lps.getExecType() == LopProperties.ExecType.SPARK) {
			System.out.println(" unprocessed spark hop!!! " + hop);

		} else if (hop instanceof DataOp && ((DataOp) hop).getDataOpType() == Hop.DataOpTypes.TRANSIENTREAD) { // tread
			int i = 0;

		} else { // other CP
			for (int i = 0; i < hop.getInput().size(); i++) {
				if (isRDD[i]) {
					Hop in = hop.getInput().get(i);
					if (!OptimizerUtils.checkSparkCollectMemoryBudget(in.getDim1(), in.getDim2(), in.getRowsInBlock(),
							in.getColsInBlock(), in.getNnz(), 0, true)) {
						cost.hdfs += OptimizerUtils.estimatePartitionedSizeExactSparsity(
								in.getDim1(), in.getDim2(), in.getRowsInBlock(), in.getColsInBlock(), in.getNnz());
					} else {
						cost.collect += OptimizerUtils.estimatePartitionedSizeExactSparsity(
								in.getDim1(), in.getDim2(), in.getRowsInBlock(), in.getColsInBlock(), in.getNnz());
					}
				}
			}
		}

		hop.setIsRDD(cost.isRDD);

		return cost;
	}

	private HopCost estimateCost(ArrayList<Hop> hops) {
		HopCost cost = new HopCost();
		for (Hop hop : hops) {
			cost.add(estimateCost(hop));
		}
		return cost;
	}

	private class HopCost {

		double broadcast = 0;

		double join = 0;

		double shuffle = 0;

		double collect = 0;

		double hdfs = 0;

		boolean isRDD = false;

		void add(HopCost cost) {
			broadcast += cost.broadcast;
			join += cost.join;
			shuffle += cost.shuffle;
			collect += cost.collect;
		}

		@Override
		public String toString() {
			return "\tbroadcast: " + broadcast
					+ "\n\tjoin: " + join
					+ "\n\tshuffle: " + shuffle
					+ "\n\tcollect: " + collect
					+ "\n\thdfs: " + hdfs;
		}

	}

}
