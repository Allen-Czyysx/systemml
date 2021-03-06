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


import java.util.Iterator;
import java.util.LinkedList;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.instructions.spark.utils.SparkUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.data.TripleIndexes;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class RmmSPInstruction extends BinarySPInstruction {

	private RmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, String opcode, String istr) {
		super(SPType.RMM, op, in1, in2, out, opcode, istr);
	}

	public static RmmSPInstruction parseInstruction( String str ) {
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if ( "rmm".equals(opcode) ) {
			CPOperand in1 = new CPOperand(parts[1]);
			CPOperand in2 = new CPOperand(parts[2]);
			CPOperand out = new CPOperand(parts[3]);
			
			return new RmmSPInstruction(null, in1, in2, out, opcode, str);
		} 
		else {
			throw new DMLRuntimeException("RmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);
		}
	}
	
	@Override
	public void processInstruction(ExecutionContext ec) {
		// TODO added by czh debug
		long t1 = System.currentTimeMillis();

		SparkExecutionContext sec = (SparkExecutionContext)ec;
		
		//get input rdds
		MatrixCharacteristics mc1 = sec.getMatrixCharacteristics( input1.getName() );
		MatrixCharacteristics mc2 = sec.getMatrixCharacteristics( input2.getName() );
		JavaPairRDD<MatrixIndexes,MatrixBlock> in1 = sec.getBinaryBlockRDDHandleForVariable( input1.getName() );
		JavaPairRDD<MatrixIndexes,MatrixBlock> in2 = sec.getBinaryBlockRDDHandleForVariable( input2.getName() );
		MatrixCharacteristics mcOut = updateBinaryMMOutputMatrixCharacteristics(sec, true);
		
		//execute Spark RMM instruction
		//step 1: prepare join keys (w/ shallow replication), i/j/k
		JavaPairRDD<TripleIndexes,MatrixBlock> tmp1 = in1.flatMapToPair(
			new RmmReplicateFunction(mc2.getCols(), mc2.getColsPerBlock(), true)); 
		JavaPairRDD<TripleIndexes,MatrixBlock> tmp2 = in2.flatMapToPair(
			new RmmReplicateFunction(mc1.getRows(), mc1.getRowsPerBlock(), false));
		
		//step 2: join prepared datasets, multiply, and aggregate
		int numPartJoin = Math.max(getNumJoinPartitions(mc1, mc2),
			SparkExecutionContext.getDefaultParallelism(true));
		int numPartOut = SparkUtils.getNumPreferredPartitions(mcOut);
		JavaPairRDD<MatrixIndexes,MatrixBlock> out = tmp1
			.join( tmp2, numPartJoin )               //join by result block 
		    .mapToPair( new RmmMultiplyFunction() ); //do matrix multiplication
		out = RDDAggregateUtils.sumByKeyStable(out,  //aggregation per result block
			numPartOut, false); 
		
		//put output block into symbol table (no lineage because single block)
		sec.setRDDHandleForVariable(output.getName(), out);
		sec.addLineageRDD(output.getName(), input1.getName());
		sec.addLineageRDD(output.getName(), input2.getName());

		// TODO added by czh debug
		sec.getMatrixInput(output.getName());
		sec.releaseMatrixInput(output.getName());
		long t2 = System.currentTimeMillis();
		System.out.println("rmm " + input1.getName() + " time: " + (t2 - t1) / 1000.0);
	}
	
	private static int getNumJoinPartitions(MatrixCharacteristics mc1, MatrixCharacteristics mc2) {
		if( !mc1.dimsKnown() || !mc2.dimsKnown() )
			SparkExecutionContext.getDefaultParallelism(true);
		//compute data size of replicated inputs
		double hdfsBlockSize = InfrastructureAnalyzer.getHDFSBlockSize();
		double matrix1PSize = OptimizerUtils.estimatePartitionedSizeExactSparsity(mc1)
			* ((long) Math.ceil((double)mc2.getCols()/mc2.getColsPerBlock()));
		double matrix2PSize = OptimizerUtils.estimatePartitionedSizeExactSparsity(mc2)
			* ((long) Math.ceil((double)mc1.getRows()/mc1.getRowsPerBlock()));
		return (int) Math.max(Math.ceil((matrix1PSize+matrix2PSize)/hdfsBlockSize), 1);
	}

	private static class RmmReplicateFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>, TripleIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = 3577072668341033932L;
		
		private long _len;
		private long _blen;
		private boolean _left;
		
		public RmmReplicateFunction(long len, long blen, boolean left)
		{
			_len = len;
			_blen = blen;
			_left = left;
		}
		
		@Override
		public Iterator<Tuple2<TripleIndexes, MatrixBlock>> call( Tuple2<MatrixIndexes, MatrixBlock> arg0 ) 
			throws Exception 
		{
			LinkedList<Tuple2<TripleIndexes, MatrixBlock>> ret = new LinkedList<>();
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();
			
			long numBlocks = (long) Math.ceil((double)_len/_blen); 
			
			if( _left ) //LHS MATRIX
			{
				//replicate wrt # column blocks in RHS
				long i = ixIn.getRowIndex();
				long k = ixIn.getColumnIndex();
				for( long j=1; j<=numBlocks; j++ ) {
					TripleIndexes tmptix = new TripleIndexes(i, j, k);
					ret.add( new Tuple2<>(tmptix, blkIn) );
				}
			} 
			else // RHS MATRIX
			{
				//replicate wrt # row blocks in LHS
				long k = ixIn.getRowIndex();
				long j = ixIn.getColumnIndex();
				for( long i=1; i<=numBlocks; i++ ) {
					TripleIndexes tmptix = new TripleIndexes(i, j, k);
					ret.add( new Tuple2<>(tmptix, blkIn) );
				}
			}
			
			//output list of new tuples
			return ret.iterator();
		}
	}

	private static class RmmMultiplyFunction implements PairFunction<Tuple2<TripleIndexes, Tuple2<MatrixBlock,MatrixBlock>>, MatrixIndexes, MatrixBlock> 
	{
		private static final long serialVersionUID = -5772410117511730911L;
		
		private AggregateBinaryOperator _op = null;
		
		public RmmMultiplyFunction()
		{
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call( Tuple2<TripleIndexes, Tuple2<MatrixBlock,MatrixBlock>> arg0 ) 
			throws Exception 
		{
			//get input blocks per
			TripleIndexes ixIn = arg0._1(); //i,j,k
			MatrixIndexes ixOut = new MatrixIndexes(ixIn.getFirstIndex(), ixIn.getSecondIndex()); //i,j
			MatrixBlock blkIn1 = arg0._2()._1();
			MatrixBlock blkIn2 = arg0._2()._2();
			
			//core block matrix multiplication 
			MatrixBlock blkOut = OperationsOnMatrixValues.matMult(blkIn1, blkIn2, new MatrixBlock(), _op);
			
			//output new tuple
			return new Tuple2<>(ixOut, blkOut);
		}
	}
}
