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

package org.apache.sysml.runtime.instructions.spark;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.sysml.parser.DWhileStatement;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.runtime.controlprogram.DWhileProgramBlock;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.instructions.spark.data.RDDObject;
import org.apache.sysml.runtime.instructions.spark.data.RowMatrixBlock;
import org.apache.sysml.runtime.instructions.spark.utils.RDDSortUtils;
import org.apache.sysml.runtime.matrix.MetaData;
import org.apache.sysml.runtime.matrix.data.*;
import scala.Tuple2;

import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.functionobjects.SwapIndex;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction;
import org.apache.sysml.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction2;
import org.apache.sysml.runtime.instructions.spark.functions.ReorgMapFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.mapred.IndexedMatrixValue;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;
import org.apache.sysml.runtime.matrix.operators.ReorgOperator;

import java.util.*;

/**
 * Cpmm: cross-product matrix multiplication operation (distributed matrix multiply
 * by join over common dimension and subsequent aggregation of partial results).
 *
 * NOTE: There is additional optimization potential by preventing aggregation for a single
 * block on the common dimension. However, in such a case we would never pick cpmm because
 * this would result in a degree of parallelism of 1.
 *
 */
public class CpmmSPInstruction extends BinarySPInstruction {
	private final boolean _outputEmptyBlocks;
	private final SparkAggType _aggtype;

	private CpmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, boolean outputEmptyBlocks,
							  SparkAggType aggtype, String opcode, String istr, boolean needCache,
							  String preOutputName, String dVarName) {
		super(SPType.CPMM, op, in1, in2, out, opcode, istr);
		_outputEmptyBlocks = outputEmptyBlocks;
		_aggtype = aggtype;
		_needCache = needCache;
		_preOutputName = preOutputName;
		_dVarNames = new String[]{dVarName};
	}

	public static CpmmSPInstruction parseInstruction( String str ) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];
		if ( !opcode.equalsIgnoreCase("cpmm"))
			throw new DMLRuntimeException("CpmmSPInstruction.parseInstruction(): Unknown opcode " + opcode);
		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);
		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		boolean outputEmptyBlocks = Boolean.parseBoolean(parts[4]);
		SparkAggType aggtype = SparkAggType.valueOf(parts[5]);

		boolean needCache = Boolean.parseBoolean(parts[6]);
		String preOutputName = null;
		String dVarName = null;
		if (needCache) {
			preOutputName = parts[7];
			if (parts.length == 10 && parts[8].equals("1")) {
				dVarName = parts[9];
			} else if (parts.length > 10) {
				throw new ParseException("Should not be here");
			}
		}

		return new CpmmSPInstruction(
				aggbin, in1, in2, out, outputEmptyBlocks, aggtype, opcode, str, needCache, preOutputName, dVarName);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		long t1 = System.currentTimeMillis(); // TODO added by czh debug

		SparkExecutionContext sec = (SparkExecutionContext) ec;

		//get rdd inputs
		String in1Name = input1.getName();
		String in2Name = input2.getName();
		String repartitionIn1Name = in1Name + "_repartition";
		MatrixCharacteristics mc1 = sec.getMatrixCharacteristics(in1Name);
		MatrixCharacteristics mc2 = sec.getMatrixCharacteristics(in2Name);
		boolean needRepartitionNow = needRepartitionNow(in2Name);
		boolean hasRepartitioned = hasRepartitioned(in1Name, in2Name);
		JavaPairRDD<MatrixIndexes, MatrixBlock> in1 = !needRepartitionNow && hasRepartitioned ?
				sec.getBinaryBlockRDDHandleForVariable(repartitionIn1Name) :
				sec.getBinaryBlockRDDHandleForVariable(in1Name);
		JavaPairRDD<MatrixIndexes, MatrixBlock> in2 = sec.getBinaryBlockRDDHandleForVariable(in2Name);
		String dVarName = null; // TODO added by czh 检查是否是dVar

		if (_needCache && DWhileStatement.isDWhileTmpVar(in2Name)) {
			dVarName = _dVarNames[0];
			System.out.println("in delta cpmm: " + dVarName);
		}

//		// TODO added by czh debug
//		needRepartitionNow = false;
//		if (sec.getScalarInput(DWhileStatement.getDwhileCountName(), Expression.ValueType.INT, false).getLongValue() == 4) {
//			needRepartitionNow = false;
//		}

		if (!_outputEmptyBlocks || _aggtype == SparkAggType.SINGLE_BLOCK) {
			//prune empty blocks of ultra-sparse matrices
			in1 = in1.filter(new FilterNonEmptyBlocksFunction());
			in2 = in2.filter(new FilterNonEmptyBlocksFunction());
		}

		if (SparkUtils.isHashPartitioned(in1) && mc1.getNumRowBlocks() == 1 && mc2.getCols() == 1) { //ZIPMM-like CPMM
			//note: if the major input is hash-partitioned and it's a matrix-vector
			//multiply, avoid the index mapping to preserve the partitioning similar
			//to a ZIPMM but with different transpose characteristics
			JavaRDD<MatrixBlock> out = in1
					.join(in2.mapToPair(new ReorgMapFunction("r'")))
					.values().map(new Cpmm2MultiplyFunction())
					.filter(new FilterNonEmptyBlocksFunction2());
			MatrixBlock out2 = RDDAggregateUtils.sumStable(out);

			//put output block into symbol table (no lineage because single block)
			//this also includes implicit maintenance of matrix characteristics
			sec.setMatrixOutput(output.getName(), out2, getExtendedOpcode());

		} else { //GENERAL CPMM
			JavaPairRDD<Long, IndexedMatrixValue> tmp1;
			JavaPairRDD<Long, IndexedMatrixValue> tmp2;

			if (needRepartitionNow) {
				System.out.println("repartitioning... " + in2Name);
				_hasRepartitioned.add(in1Name);

				if (sec.containsVariable(repartitionIn1Name)) {
					sec.cleanupCacheableData(sec.getMatrixObject(repartitionIn1Name));
				}

				// repartitionNonZeros in2
				MatrixBlock order = LibMatrixReorg.getOrderWithSelect(sec, true, dVarName, in1.getNumPartitions(), mc1);
				if (order == null) {
					skip(sec);
					return;
				}
				String orderName = DWhileStatement.getRepartitionOrderName(dVarName);
				PartitionedBroadcast<MatrixBlock> orderPb = sec.getBroadcastForVariable(orderName);
				tmp2 = LibMatrixReorg.repartitionIn2ByOrderSP(in2, mc2, orderPb)
						.mapToPair(new CpmmIndexFunction(false));

				// repartitionNonZeros in1
				tmp1 = LibMatrixReorg.repartitionIn1ByOrderSP(sec, in1, dVarName, in1Name, repartitionIn1Name)
						.mapToPair(new CpmmIndexFunction(true));

			} else if (hasRepartitioned) {
				// 若在之前的迭代已repartition in1, 则调整 in2 顺序
				MatrixBlock order = LibMatrixReorg.getNewOrderWithSelect(sec, true, dVarName);
				if (order == null) {
					skip(sec);
					return;
				}

				String newOrderName = DWhileStatement.getNewRepartitionOrderName(dVarName);
				PartitionedBroadcast<MatrixBlock> newOrderPb = sec.getBroadcastForVariable(newOrderName);
				tmp2 = LibMatrixReorg.repartitionIn2ByOrderSP(in2, mc2, newOrderPb)
						.mapToPair(new CpmmIndexFunction(false));

				tmp1 = in1.mapToPair(new CpmmIndexFunction(true));

			} else if (_needCache && DWhileStatement.isDWhileTmpVar(in2Name)) {
				PartitionedBroadcast<MatrixBlock> selectPb = getSelectPb(sec, in2Name, dVarName);
				if (selectPb == null) {
					skip(sec);
					return;
				}

				tmp1 = in1.flatMapToPair(new CpmmIndexSelectFunction(true, selectPb));
				tmp2 = in2.flatMapToPair(new CpmmIndexSelectFunction(false, selectPb));

			} else {
				tmp1 = in1.mapToPair(new CpmmIndexFunction(true));
				tmp2 = in2.mapToPair(new CpmmIndexFunction(false));
			}

			//compute preferred join degree of parallelism
			int numPreferred = getPreferredParJoin(mc1, mc2, in1.getNumPartitions(), in2.getNumPartitions());
			int numPartJoin = Math.min(getMaxParJoin(mc1, mc2), numPreferred);

			//process core cpmm matrix multiply
			JavaPairRDD<MatrixIndexes, MatrixBlock> out = tmp1.join(tmp2, numPartJoin) // join over common dimension
					.mapToPair(new CpmmMultiplyFunction()); // compute block multiplications

			System.out.println("cpmm partition count: " + out.getNumPartitions()); // TODO added by czh debug

			//process cpmm aggregation and handle outputs
			if (_aggtype == SparkAggType.SINGLE_BLOCK) {
				//prune empty blocks and aggregate all results
				out = out.filter(new FilterNonEmptyBlocksFunction());
				MatrixBlock out2 = RDDAggregateUtils.sumStable(out);

				//put output block into symbol table (no lineage because single block)
				//this also includes implicit maintenance of matrix characteristics
				sec.setMatrixOutput(output.getName(), out2, getExtendedOpcode());
			} else { //DEFAULT: MULTI_BLOCK
				if (!_outputEmptyBlocks)
					out = out.filter(new FilterNonEmptyBlocksFunction());
				out = RDDAggregateUtils.sumByKeyStable(out, false);

				//put output RDD handle into symbol table
				sec.setRDDHandleForVariable(output.getName(), out);
				sec.addLineageRDD(output.getName(),
						needRepartitionNow || hasRepartitioned ? repartitionIn1Name : in1Name);
				sec.addLineageRDD(output.getName(), in2Name);

				//update output statistics if not inferred
				updateBinaryMMOutputMatrixCharacteristics(sec, true);
			}
		}

		if (!needRepartitionNow && hasRepartitioned) {
			String newOrderName = DWhileStatement.getNewRepartitionOrderName(dVarName);
			sec.addLineageBroadcast(output.getName(), newOrderName);
		} else if (!needRepartitionNow && _needCache && DWhileStatement.isDWhileTmpVar(in2Name)) {
			String selectBlockName = DWhileStatement.getSelectBlockName(dVarName);
			sec.addLineageBroadcast(output.getName(), selectBlockName);
		}

		if (needRepartitionNow) {
			sec.unpersistRdd(repartitionIn1Name);
		}

//		// TODO added by czh debug
////		sec.getMatrixInput(output.getName());
////		sec.releaseMatrixInput(output.getName());
//		sec.getMatrixObject(output.getName()).getRDDHandle().getRDD().count();
//		System.out.println("cpmm " + input1.getName() + " time: " + (System.currentTimeMillis() - t1) / 1000.0);
	}

	private static int getPreferredParJoin(MatrixCharacteristics mc1, MatrixCharacteristics mc2, int numPar1, int numPar2) {
		int defPar = SparkExecutionContext.getDefaultParallelism(true);
		int maxParIn = Math.max(numPar1, numPar2);
		int maxSizeIn = SparkUtils.getNumPreferredPartitions(mc1) +
			SparkUtils.getNumPreferredPartitions(mc2);
		int tmp = (mc1.dimsKnown(true) && mc2.dimsKnown(true)) ?
			Math.max(maxSizeIn, maxParIn) : maxParIn;
		return (tmp > defPar/2) ? Math.max(tmp, defPar) : tmp;
	}

	private static int getMaxParJoin(MatrixCharacteristics mc1, MatrixCharacteristics mc2) {
		return mc1.colsKnown() ? (int)mc1.getNumColBlocks() :
			mc2.rowsKnown() ? (int)mc2.getNumRowBlocks() :
			Integer.MAX_VALUE;
	}

	private PartitionedBroadcast<MatrixBlock> getSelectPb(SparkExecutionContext sec, String in2Name, String dVarName) {
		FilterBlock selectBlock = selectMatrixBlock(sec, in2Name, dVarName);

		if (selectBlock == null) {
			return null;
		}

//		// TODO addded by czh debug
//		for (int i = 0; i < 100; i++) {
//			selectBlock.setData(i, 0, 0);
//		}

		MatrixBlock select = new MatrixBlock(1, 1, true);
		select.setSelectBlock(selectBlock);
		String selectBlockName = DWhileStatement.getSelectBlockName(dVarName);
		if (!sec.containsVariable(selectBlockName)) {
			MatrixCharacteristics mc = new MatrixCharacteristics(1, 1, 1, 1, 0);
			MatrixObject mo = new MatrixObject(null, null, new MetaData(mc));
			mo.enableCleanup(true);
			sec.setVariable(selectBlockName, mo);
		}
		sec.setMatrixOutput(selectBlockName, select);

		return sec.getBroadcastForVariable(selectBlockName);
	}

	private void skip(SparkExecutionContext sec) {
		MatrixCharacteristics outputMc = sec.getMatrixCharacteristics(output.getName());
		sec.setMatrixOutput(output.getName(),
				new MatrixBlock((int) outputMc.getRows(), (int) outputMc.getCols(), true), getExtendedOpcode());

		if (_aggtype != SparkAggType.SINGLE_BLOCK) {
			updateBinaryMMOutputMatrixCharacteristics(sec, true);
		}

		// TODO added by czh debug
		System.out.println("skip cpmm " + input1.getName());
	}

	public static class CpmmIndexFunction implements
			PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, Long, IndexedMatrixValue> {

		private static final long serialVersionUID = -1187183128301671162L;

		private final boolean _left;

		public CpmmIndexFunction(boolean left) {
			_left = left;
		}

		@Override
		public Tuple2<Long, IndexedMatrixValue> call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
			IndexedMatrixValue value = new IndexedMatrixValue(arg0._1(), arg0._2());
			Long key = _left ? arg0._1.getColumnIndex() : arg0._1.getRowIndex();
			return new Tuple2<>(key, value);
		}

	}

	private static class CpmmIndexSelectFunction implements
			PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, Long, IndexedMatrixValue> {

		private static final long serialVersionUID = -1187183128301671162L;

		private final boolean _left;

		private final PartitionedBroadcast<MatrixBlock> _pbc;

		public CpmmIndexSelectFunction(boolean left, PartitionedBroadcast<MatrixBlock> binput) {
			_left = left;
			_pbc = binput;
		}

		@Override
		public Iterator<Tuple2<Long, IndexedMatrixValue>> call(Tuple2<MatrixIndexes, MatrixBlock> arg)
				throws Exception {
			MatrixIndexes idx = arg._1;
			MatrixBlock block = arg._2;
			long key = _left ? idx.getColumnIndex() : idx.getRowIndex();
			FilterBlock select = _pbc.getBlock(1, 1).getSelectBlock();

			if (select.getData((int) key - 1, 0) == 0) {
				return new ArrayList<Tuple2<Long, IndexedMatrixValue>>(0).iterator();
			}

			IndexedMatrixValue value = new IndexedMatrixValue(idx, block);
			List<Tuple2<Long, IndexedMatrixValue>> list = new ArrayList<>(1);
			list.add(new Tuple2<>(key, value));
			return list.iterator();
		}

	}

	private static class CpmmMultiplyFunction implements PairFunction<Tuple2<Long, Tuple2<IndexedMatrixValue,IndexedMatrixValue>>, MatrixIndexes, MatrixBlock>
	{
		private static final long serialVersionUID = -2009255629093036642L;
		private AggregateBinaryOperator _op = null;

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<Long, Tuple2<IndexedMatrixValue, IndexedMatrixValue>> arg0)
			throws Exception
		{
			if( _op == null ) { //lazy operator construction
				AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
				_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
			}

			MatrixBlock blkIn1 = (MatrixBlock)arg0._2()._1().getValue();
			MatrixBlock blkIn2 = (MatrixBlock)arg0._2()._2().getValue();
			MatrixIndexes ixOut = new MatrixIndexes();

			//core block matrix multiplication 
			MatrixBlock blkOut = OperationsOnMatrixValues
				.matMult(blkIn1, blkIn2, new MatrixBlock(), _op);

			//return target block
			ixOut.setIndexes(arg0._2()._1().getIndexes().getRowIndex(),
				arg0._2()._2().getIndexes().getColumnIndex());
			return new Tuple2<>( ixOut, blkOut );
		}
	}

	private static class Cpmm2MultiplyFunction implements Function<Tuple2<MatrixBlock,MatrixBlock>, MatrixBlock>
	{
		private static final long serialVersionUID = -3718880362385713416L;
		private AggregateBinaryOperator _op = null;
		private ReorgOperator _rop = null;

		@Override
		public MatrixBlock call(Tuple2<MatrixBlock, MatrixBlock> arg0) throws Exception {
			 //lazy operator construction
			if( _op == null ) {
				AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
				_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
				_rop = new ReorgOperator(SwapIndex.getSwapIndexFnObject());
			}
			//prepare inputs, including transpose of right-hand-side
			MatrixBlock in1 = arg0._1();
			MatrixBlock in2 = (MatrixBlock)arg0._2()
				.reorgOperations(_rop, new MatrixBlock(), 0, 0, 0);
			//core block matrix multiplication
			return OperationsOnMatrixValues
				.matMult(in1, in2, new MatrixBlock(), _op);
		}
	}

	public static class FilterNonEmptyRowBlocksFunction implements
			Function<Tuple2<MatrixIndexes, RowMatrixBlock>, Boolean> {

		private static final long serialVersionUID = -8856829325565589854L;

		@Override
		public Boolean call(Tuple2<MatrixIndexes, RowMatrixBlock> arg0) throws Exception {
			return arg0._2().getRow() != -1;
		}
	}

}
