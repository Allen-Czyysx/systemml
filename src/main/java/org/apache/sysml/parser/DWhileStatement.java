package org.apache.sysml.parser;

import org.apache.sysml.hops.Hop;

import java.util.ArrayList;

public class DWhileStatement extends WhileStatement {

	private String[] _dVarNames;

	private ArrayList<StatementBlock> _dIterInit;

	private ArrayList<StatementBlock> _dIterBefore;

	private ArrayList<StatementBlock> _dIterAfter;

	public String[] getDVarNames() {
		return _dVarNames;
	}

	public void setDVarNames(String[] dVarName) {
		_dVarNames = dVarName;
	}

	public ArrayList<StatementBlock> getDIterInit() {
		return _dIterInit;
	}

	public StatementBlock getDIterInitByIndex(int i) {
		return _dIterInit.get(i);
	}

	public void setDIterInit(ArrayList<StatementBlock> dIterInit) {
		_dIterInit = dIterInit;
	}

	public ArrayList<StatementBlock> getDIterBefore() {
		return _dIterBefore;
	}

	public StatementBlock getDIterBeforeByIndex(int i) {
		return _dIterBefore.get(i);
	}

	public void setDIterBefore(ArrayList<StatementBlock> dIterBefore) {
		_dIterBefore = dIterBefore;
	}

	public ArrayList<StatementBlock> getDIterAfter() {
		return _dIterAfter;
	}

	public StatementBlock getDIterAfterByIndex(int i) {
		return _dIterAfter.get(i);
	}

	public void setDIterAfter(ArrayList<StatementBlock> dIterAfter) {
		_dIterAfter = dIterAfter;
	}

	public static boolean isDWhileTmpVar(String varName) {
		if (varName == null) {
			return false;
		}
		return Character.isDigit(varName.charAt(0));
	}

	public static String getPreVarName(String varName) {
		return "1_preVar_" + varName;
	}

	public static String getVarUseDeltaName(String varName) {
		return "2_useDelta_" + varName;
	}

	public static String getSelectName(String varName) {
		return "3_select_" + varName;
	}

	public static String getDeltaName(String varName) {
		return "4_delta_" + varName;
	}

	public static String getPreOutputNameFromHop(Hop hop) {
		return "5_preOutput_hop_" + hop.getName() + "_" + hop.getOpString() + "_" + hop.getBeginLine() +
				"_" + hop.getBeginColumn() + "_" + hop.getEndLine() + "_" + hop.getEndColumn();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("dwhile (");
		sb.append(_predicate);
		sb.append(" @ ");
		for (String name : _dVarNames) {
			sb.append(name);
			sb.append(", ");
		}
		sb.append(") {\n");
		for (StatementBlock block : _body) {
			sb.append(block.toString());
		}
		sb.append("}\n");
		return sb.toString();
	}

}
