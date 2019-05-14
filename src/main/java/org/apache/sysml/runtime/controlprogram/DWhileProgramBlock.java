package org.apache.sysml.runtime.controlprogram;

import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLScriptException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.Instruction;

import java.util.ArrayList;

// TODO added by czh 退出dwhile时要删除多余变量
public class DWhileProgramBlock extends WhileProgramBlock {

	private ArrayList<ProgramBlock> _dIterInit;

	private ArrayList<ProgramBlock> _dIterBefore;

	private ArrayList<ProgramBlock> _dIterAfter;

	public DWhileProgramBlock(Program prog, ArrayList<Instruction> predicate) {
		super(prog, predicate);
		_dIterInit = new ArrayList<>();
		_dIterBefore = new ArrayList<>();
		_dIterAfter = new ArrayList<>();
	}

	public ArrayList<ProgramBlock> getDIterInit() {
		return _dIterInit;
	}

	public ArrayList<ProgramBlock> getDIterBefore() {
		return _dIterBefore;
	}

	public ArrayList<ProgramBlock> getDIterAfter() {
		return _dIterAfter;
	}

	public void setDIterInit(ArrayList<ProgramBlock> dIterInit) {
		_dIterInit = dIterInit;
	}

	public void setDIterBefore(ArrayList<ProgramBlock> dIterBefore) {
		_dIterBefore = dIterBefore;
	}

	public void setDIterAfter(ArrayList<ProgramBlock> dIterAfter) {
		_dIterAfter = dIterAfter;
	}

	public void addDIterInit(ProgramBlock pb) {
		_dIterInit.add(pb);
	}

	public void addDIterBefore(ProgramBlock pb) {
		_dIterBefore.add(pb);
	}

	public void addDIterAfter(ProgramBlock pb) {
		_dIterAfter.add(pb);
	}

	@Override
	public void execute(ExecutionContext ec) {
		// execute while loop
		try {
			// prepare update in-place variables
			MatrixObject.UpdateType[] flags = prepareUpdateInPlaceVariables(ec, _tid);

			// 执行init
			for (ProgramBlock pb : _dIterInit) {
				pb.execute(ec);
			}

			// run loop body until predicate becomes false
			int count = 1;
			while (executePredicate(ec).getBooleanValue()) {
				long t1 = System.currentTimeMillis();

				// 执行before
				for (ProgramBlock pb : _dIterBefore) {
					pb.execute(ec);
				}

				long t2 = System.currentTimeMillis();

				// execute all child blocks
				for (int i = 0; i < _childBlocks.size(); i++) {
					ec.updateDebugState(i);
					_childBlocks.get(i).execute(ec);
				}

				long t3 = System.currentTimeMillis();

				// 执行after
				for (ProgramBlock pb : _dIterAfter) {
					pb.execute(ec);
				}

				long t4 = System.currentTimeMillis();
				System.out.println("child \t" + (t3 - t2) / 1000.0);
				System.out.println("after \t" + (t4 - t3) / 1000.0);
				System.out.println("dwhile\t" + (t4 - t1) / 1000.0 + "\t" + count + "\n");
				count++;
			}

			// reset update-in-place variables
			resetUpdateInPlaceVariableFlags(ec, flags);
		} catch (DMLScriptException e) {
			// propagate stop call
			throw e;
		} catch (Exception e) {
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error evaluating while program block", e);
		}

		// execute exit instructions
		try {
			executeInstructions(_exitInstructions, ec);
		} catch (Exception e) {
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error executing while exit instructions.", e);
		}
	}

	@Override
	public String printBlockErrorLocation() {
		return "ERROR: Runtime error in dwhile program block generated from dwhile statement block between lines "
				+ _beginLine + " and " + _endLine + " -- ";
	}

}
