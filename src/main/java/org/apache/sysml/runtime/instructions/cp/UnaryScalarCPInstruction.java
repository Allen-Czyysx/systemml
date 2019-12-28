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
import org.apache.sysml.api.ScriptExecutorUtils;
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
import org.apache.sysml.runtime.instructions.spark.data.ColPartitioner;
import org.apache.sysml.runtime.instructions.spark.data.RowPartitioner;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.UnaryOperator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.sysml.conf.DMLConfig.*;
import static org.apache.sysml.hops.OptimizerUtils.DEFAULT_BLOCKSIZE;
import static org.apache.sysml.hops.recompile.Recompiler.*;

public class UnaryScalarCPInstruction extends UnaryMatrixCPInstruction {

	protected UnaryScalarCPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String instr) {
		super(op, in, out, opcode, instr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		String opcode = getOpcode();
		ScalarObject sores;

		//get the scalar input 
		ScalarObject so = ec.getScalarInput(input1);

		//core execution
		if (DWhileStatement.isVarUseDeltaName(input1.getName()) && opcode.equals("!")) {
			String dVarName = DWhileStatement.getDVarNameFromTmpVar(input1.getName());
			DWhileProgramBlock pb = DWhileProgramBlock._pb;
			if (pb == null) {
				throw new DMLRuntimeException("DWhileProgramBlock._pb shouldn't be null");
			}

			List<ProgramBlock> comPb = new ArrayList<>();
			List<ProgramBlock> incPb = new ArrayList<>();
			List<ProgramBlock> aftPb = new ArrayList<>();

			IfProgramBlock mainPb = (IfProgramBlock) (pb.getChildBlocks().get(0));
			VariableCPInstruction pred = (VariableCPInstruction) mainPb.getPredicate().get(0);
			ProgramBlock ifBody = mainPb.getChildBlocksIfBody().get(0);
			ProgramBlock elseBody = mainPb.getChildBlocksElseBody().get(0);
			if (DWhileStatement.getDVarNameFromTmpVar(pred.getInput(0).getName()).equals(dVarName)) {
				if (ifBody instanceof IfProgramBlock && ((IfProgramBlock) ifBody).getPredicate().size() == 1) {
					if (dVarName.equals("W")) {
						IfProgramBlock tmpIfPb =
								(IfProgramBlock) ((IfProgramBlock) ifBody).getChildBlocksElseBody().get(0);
						incPb.add(tmpIfPb.getChildBlocksIfBody().get(0));
						tmpIfPb =
								(IfProgramBlock) ((IfProgramBlock) elseBody).getChildBlocksElseBody().get(0);
						comPb.add(tmpIfPb.getChildBlocksIfBody().get(0));

					} else {
						incPb.add(((IfProgramBlock) ifBody).getChildBlocksElseBody().get(0));
						comPb.add(((IfProgramBlock) elseBody).getChildBlocksElseBody().get(0));
					}

				} else {
					incPb.add(ifBody);
					comPb.add(elseBody);
				}

			} else {
				if (dVarName.equals("H")) {
					IfProgramBlock tmpIfPb =
							(IfProgramBlock) ((IfProgramBlock) elseBody).getChildBlocksIfBody().get(0);
					incPb.add(tmpIfPb.getChildBlocksElseBody().get(0));
					tmpIfPb =
							(IfProgramBlock) ((IfProgramBlock) elseBody).getChildBlocksElseBody().get(0);
					comPb.add(tmpIfPb.getChildBlocksElseBody().get(0));

				} else {
					incPb.add(((IfProgramBlock) elseBody).getChildBlocksIfBody().get(0));
					comPb.add(((IfProgramBlock) elseBody).getChildBlocksElseBody().get(0));
				}
			}

			for (ProgramBlock tmpPb : pb.getDIterAfter()) {
				if (tmpPb instanceof IfProgramBlock) {
					VariableCPInstruction tmpPred =
							(VariableCPInstruction) ((IfProgramBlock) tmpPb).getPredicate().get(0);
					if (DWhileStatement.getDVarNameFromTmpVar(tmpPred.getInput(0).getName()).equals(dVarName)) {
						aftPb.add(tmpPb);
					}
				} else {
					aftPb.add(tmpPb);
				}
			}

			ArrayList<Hop> newComHops = recompileProgramBlock(ec, comPb);
			ArrayList<Hop> newIncHops = recompileProgramBlock(ec, incPb);
			ArrayList<Hop> newAftHops = recompileProgramBlock(ec, aftPb);

			HopCost comCost = estimateCost(newComHops);
			HopCost incCost = estimateCost(newIncHops);
			HopCost aftCost = estimateCost(newAftHops);

			System.out.println("com" + comCost);
			System.out.println("inc" + incCost);
			System.out.println("aft" + aftCost);
			System.out.println("inc total: " + (incCost.getCost() + aftCost.getCost()));
			System.out.println("com total: " + comCost.getCost());


			sores = new BooleanObject(incCost.getCost() + aftCost.getCost() < comCost.getCost());

			if (incCost.getCost() + aftCost.getCost() < comCost.getCost()) {
				int detectStep = ScriptExecutorUtils.dmlConfig.getIntValue(DETECTION_STEP);
				if (detectStep < 1) {
//					DWhileProgramBlock._detectStepHistory.putIfAbsent()
				}
				System.out.println("use delta next time");
			}

		} else if (DWhileStatement.isDetectName(input1.getName()) && opcode.equals("!")) {
			sores = isDetect(ec, input1.getName());

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

	private ArrayList<Hop> recompileProgramBlock(ExecutionContext ec, List<ProgramBlock> pbs) {
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

			double interS = 1 - Math.pow(1 - in1s * in2s, in1c);
			if (useRepartition) {
				String dVarName = DWhileStatement.getDVarNameFromTmpVar(in2.getName());
				String blockNumName = DWhileStatement.getRepartitionBlockNumName(dVarName);
				double blockNum = ec.getScalarInput(blockNumName, ValueType.INT, false).getDoubleValue();
				interS *= blockNum / Math.ceil(1.0 * in2r / DEFAULT_BLOCKSIZE);
			}

			long m2SizeP = OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in2r, in2c, in2.getRowsInBlock(), in2.getColsInBlock(), in2s);
			long mInterSizeP = OptimizerUtils.estimatePartitionedSizeExactSparsity(
					in1r, in2c, in1.getRowsInBlock(), in1.getColsInBlock(), interS);

			if (lop instanceof MMCJ) { // CPMM
				cost.join += m2SizeP;

				int taskNum = ec.getMatrixObject("A").getRDDHandle().getRDD().getNumPartitions();
				cost.shuffle += mInterSizeP * taskNum;
//				cost.shuffle += mInterSizeP * 6;

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

				int taskNum = ec.getMatrixObject("A").getRDDHandle().getRDD().getNumPartitions();
				MatrixCharacteristics inMc =
						new MatrixCharacteristics(in1.getDim1(), in1.getDim2(), 1000, 1000);
				RowPartitioner p = new RowPartitioner(inMc, taskNum);
				cost.shuffle += mInterSizeP * p._ncparts;

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

	private static ScalarObject isDetect(ExecutionContext ec, String name) {
		return isDetect(ec, name, 0);
	}

	public static ScalarObject isDetect(ExecutionContext ec, String name, int delta) {
		String dVarName = DWhileStatement.getDVarNameFromTmpVar(name);
		long dwhileCount = ec.getScalarInput(DWhileStatement.getDwhileCountName(), ValueType.INT, false)
				.getLongValue() + delta;
		ScalarObject sores =  new BooleanObject(isDetect(dVarName, dwhileCount));

		if (sores.getBooleanValue()) {
			System.out.println("detecting..." + dVarName);
		}

		return sores;
	}

	public static boolean isDetect(String dVarName, long dwhileCount) {
		int detectStart = -1;
		String[] detectStartConfs = ScriptExecutorUtils.dmlConfig.getTextValue(DETECTION_START).split(",");
		for (String conf : detectStartConfs) {
			String[] map = conf.split(":");
			if (map.length == 2 && map[0].equals(dVarName)) {
				detectStart = Integer.parseInt(map[1]);
				break;
			}
		}

		if (dwhileCount < detectStart) {
			return false;
		}

		// TODO added by czh 特殊处理
		int detectStep = Math.max(ScriptExecutorUtils.dmlConfig.getIntValue(DETECTION_STEP), 1);
		if (dVarName.equals("au")) {
			return dwhileCount % (2 * detectStep) == 0;
		} else if (dVarName.equals("ho")) {
			return dwhileCount % (2 * detectStep) == 1;
		} else if (dVarName.equals("W")) {
			return dwhileCount % (4 * detectStep) == 1;
		} else if (dVarName.equals("H")) {
			return dwhileCount % (4 * detectStep) == 0 && dwhileCount != 0;
		} else {
			return dwhileCount % detectStep == 0;
		}
	}

	private static boolean getIsDetectWithCount(String dVarName) {
		int detectStep = Math.max(ScriptExecutorUtils.dmlConfig.getIntValue(DETECTION_STEP), 1);
		DWhileProgramBlock._detectStepCount.putIfAbsent(dVarName, detectStep);

		int detectCount = DWhileProgramBlock._detectStepCount.get(dVarName) - 1;
		if (detectCount > 0) {
			DWhileProgramBlock._detectStepCount.put(dVarName, detectCount);
			return false;
		}
		DWhileProgramBlock._detectStepCount.put(dVarName, detectStep);
		return true;
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

		double getCost() {
			return broadcast * ScriptExecutorUtils.dmlConfig.getDoubleValue(COST_BROADCAST)
					+ collect * ScriptExecutorUtils.dmlConfig.getDoubleValue(COST_COLLECT)
					+ join * ScriptExecutorUtils.dmlConfig.getDoubleValue(COST_JOIN)
					+ shuffle * ScriptExecutorUtils.dmlConfig.getDoubleValue(COST_SHUFFLE)
					+ hdfs * ScriptExecutorUtils.dmlConfig.getDoubleValue(COST_HDFS);
		}

		@Override
		public String toString() {
			return "\tbroadcast:" + broadcast
					+ "\tjoin:" + join
					+ "\tshuffle:" + shuffle
					+ "\tcollect:" + collect
					+ "\thdfs:" + hdfs;
		}

	}

}
