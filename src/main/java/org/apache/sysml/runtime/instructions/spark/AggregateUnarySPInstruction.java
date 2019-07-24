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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.sysml.runtime.functionobjects.PlusBlock;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.matrix.data.FilterBlock;
import scala.Tuple2;

import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.lops.PartialAggregate.CorrectionLocationType;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.functions.AggregateDropCorrectionFunction;
import org.apache.sysml.runtime.instructions.spark.functions.FilterDiagBlocksFunction;
import org.apache.sysml.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateUnaryOperator;

import java.util.Arrays;

public class AggregateUnarySPInstruction extends UnarySPInstruction {
	private SparkAggType _aggtype = null;
	private AggregateOperator _aop = null;

	protected AggregateUnarySPInstruction(SPType type, AggregateUnaryOperator auop, AggregateOperator aop, CPOperand in,
			CPOperand out, SparkAggType aggtype, String opcode, String istr) {
		super(type, auop, in, out, opcode, istr);
		_aggtype = aggtype;
		_aop = aop;
	}

	public static AggregateUnarySPInstruction parseInstruction(String str) {
		String[] parts = InstructionUtils.getInstructionPartsWithValueType(str);
		InstructionUtils.checkNumFields(parts, 3);
		String opcode = parts[0];

		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand out = new CPOperand(parts[2]);
		SparkAggType aggtype = SparkAggType.valueOf(parts[3]);
		
		String aopcode = InstructionUtils.deriveAggregateOperatorOpcode(opcode);
		CorrectionLocationType corrLoc = InstructionUtils.deriveAggregateOperatorCorrectionLocation(opcode);
		String corrExists = (corrLoc != CorrectionLocationType.NONE) ? "true" : "false";
		
		AggregateUnaryOperator aggun = InstructionUtils.parseBasicAggregateUnaryOperator(opcode);
		if (aggun.aggOp.increOp.fn instanceof PlusBlock) {
			aggun.aggOp.increOp.fn = null;
		}
		AggregateOperator aop = InstructionUtils.parseAggregateOperator(aopcode, corrExists, corrLoc.toString());
		return new AggregateUnarySPInstruction(SPType.AggregateUnary, aggun, aop, in1, out, aggtype, opcode, str);
	}
	
	@Override
	public void processInstruction( ExecutionContext ec ) {
		SparkExecutionContext sec = (SparkExecutionContext) ec;
		MatrixCharacteristics mc = sec.getMatrixCharacteristics(input1.getName());

		//get input
		JavaPairRDD<MatrixIndexes, MatrixBlock> in = sec.getBinaryBlockRDDHandleForVariable(input1.getName());
		JavaPairRDD<MatrixIndexes, MatrixBlock> out = in;

		//filter input blocks for trace
		if (getOpcode().equalsIgnoreCase("uaktrace"))
			out = out.filter(new FilterDiagBlocksFunction());

		//execute unary aggregate operation
		AggregateUnaryOperator auop = (AggregateUnaryOperator) _optr;
		AggregateOperator aggop = _aop;

		//perform aggregation if necessary and put output into symbol table
		if (_aggtype == SparkAggType.SINGLE_BLOCK) {
			if (aggop.increOp.fn.isBlockFn()) {
				System.out.println("Should not be here on Clusters");
				if (aggop.increOp.fn instanceof PlusBlock) {
					// TODO added by czh 当前为按行, 即一行中只要有一个非零, 就加1
					MatrixBlock mb = sec.getBlockForVariable(input1.getName());
					FilterBlock filterBlock = mb.getFilterBlock();
					int sum = 0;
					for (int i = 0; i < filterBlock.getRowNum(); i++) {
						for (int j = 0; j < filterBlock.getColNum(); j++) {
							if (filterBlock.getData(i, j) == 1) {
								sum += 1;
								break;
							}
						}
					}
					MatrixBlock outMb = new MatrixBlock(1, 1, false, 1);
					outMb.allocateDenseBlock();
					outMb.getDenseBlock().set(sum);
					sec.setMatrixOutput(output.getName(), outMb, getExtendedOpcode());
				}

			} else {
				if (auop.sparseSafe)
					out = out.filter(new FilterNonEmptyBlocksFunction());

				JavaRDD<MatrixBlock> out2 = out.map(
						new RDDUAggFunction2(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
				MatrixBlock out3 = RDDAggregateUtils.aggStable(out2, aggop);

				//drop correction after aggregation
				out3.dropLastRowsOrColumns(aggop.correctionLocation);

				//put output block into symbol table (no lineage because single block)
				//this also includes implicit maintenance of matrix characteristics
				sec.setMatrixOutput(output.getName(), out3, getExtendedOpcode());
			}

		} else { //MULTI_BLOCK or NONE
			if (_aggtype == SparkAggType.NONE) {
				//in case of no block aggregation, we always drop the correction as well as
				//use a partitioning-preserving mapvalues 
				out = out.mapValues(new RDDUAggValueFunction(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
			} else if (_aggtype == SparkAggType.MULTI_BLOCK) {
				//in case of multi-block aggregation, we always keep the correction
				out = out.mapToPair(new RDDUAggFunction(auop, mc.getRowsPerBlock(), mc.getColsPerBlock()));
				out = RDDAggregateUtils.aggByKeyStable(out, aggop, false);

				//drop correction after aggregation if required (aggbykey creates 
				//partitioning, drop correction via partitioning-preserving mapvalues)
				if (auop.aggOp.correctionExists)
					out = out.mapValues(new AggregateDropCorrectionFunction(aggop));
			}

			//put output RDD handle into symbol table
			updateUnaryAggOutputMatrixCharacteristics(sec, auop.indexFn);
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), input1.getName());
		}
	}

	private static class RDDUAggFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 2672082409287856038L;
		
		private AggregateUnaryOperator _op = null;
		private int _brlen = -1;
		private int _bclen = -1;
		
		public RDDUAggFunction( AggregateUnaryOperator op, int brlen, int bclen )
		{
			_op = op;
			_brlen = brlen;
			_bclen = bclen;
		}
		
		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			MatrixIndexes ixOut = new MatrixIndexes();
			MatrixBlock blkOut = new MatrixBlock();
			
			//unary aggregate operation (always keep the correction)
			OperationsOnMatrixValues.performAggregateUnary( ixIn, blkIn, 
					ixOut, blkOut, _op, _brlen, _bclen);
			
			//output new tuple
			return new Tuple2<>(ixOut, blkOut);
		}
	}

	/**
	 * Similar to RDDUAggFunction but single output block.
	 */
	public static class RDDUAggFunction2 implements Function<Tuple2<MatrixIndexes, MatrixBlock>, MatrixBlock> 
	{
		private static final long serialVersionUID = 2672082409287856038L;
		
		private AggregateUnaryOperator _op;
		private int _brlen;
		private int _bclen;
		
		public RDDUAggFunction2( AggregateUnaryOperator op, int brlen, int bclen ) {
			_op = op;
			_brlen = brlen;
			_bclen = bclen;
		}
		
		@Override
		public MatrixBlock call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			if (_op.aggOp.increOp.fn != null) {
				//unary aggregate operation (always keep the correction)
				return (MatrixBlock) arg0._2.aggregateUnaryOperations(
						_op, new MatrixBlock(), _brlen, _bclen, arg0._1());
			}
			return arg0._2;
		}
	}

	private static class RDDUAggValueFunction implements Function<MatrixBlock, MatrixBlock> 
	{
		private static final long serialVersionUID = 5352374590399929673L;
		
		private AggregateUnaryOperator _op = null;
		private int _brlen = -1;
		private int _bclen = -1;
		private MatrixIndexes _ix = null;
		
		public RDDUAggValueFunction( AggregateUnaryOperator op, int brlen, int bclen )
		{
			_op = op;
			_brlen = brlen;
			_bclen = bclen;
			
			_ix = new MatrixIndexes(1,1);
		}
		
		@Override
		public MatrixBlock call( MatrixBlock arg0 ) 
			throws Exception 
		{
			MatrixBlock blkOut = new MatrixBlock();
			
			//unary aggregate operation
			arg0.aggregateUnaryOperations(_op, blkOut, _brlen, _bclen, _ix);
			
			//always drop correction since no aggregation
			blkOut.dropLastRowsOrColumns(_op.aggOp.correctionLocation);
			
			//output new tuple
			return blkOut;
		}
	}
}
