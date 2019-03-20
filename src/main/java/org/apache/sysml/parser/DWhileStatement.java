package org.apache.sysml.parser;

import org.apache.sysml.hops.Hop;

import java.util.ArrayList;

public class DWhileStatement extends WhileStatement {

	private String[] _dVarNames;

	private ArrayList<Statement> _dIterBefore;

	private ArrayList<Statement> _dIterAfter;

	public String[] getDVarNames() {
		return _dVarNames;
	}

	public void setDVarNames(String[] dVarName) {
		_dVarNames = dVarName;
	}

	public ArrayList<Statement> getDIterBefore() {
		return _dIterBefore;
	}

	public Statement getDIterBeforeByIndex(int i) {
		return _dIterBefore.get(i);
	}

	public int getNumDIterBefore(){
		return _dIterBefore.size();
	}

	public void setDIterBefore(ArrayList<Statement> dIterBefore) {
		_dIterBefore = dIterBefore;
	}

	public ArrayList<Statement> getDIterAfter() {
		return _dIterAfter;
	}

	public Statement getDIterAfterByIndex(int i) {
		return _dIterAfter.get(i);
	}

	public int getNumDIterAfter(){
		return _dIterAfter.size();
	}

	public void setDIterAfter(ArrayList<Statement> dIterAfter) {
		_dIterAfter = dIterAfter;
	}

	public static String getDVarPreName(String dVarName) {
		return "systemml_d_pre_" + dVarName;
	}

	public static String getDVarDeltaName(String dVarName) {
		return "systemml_d_delta" + dVarName;
	}

	public static String getVarPreName(Hop hop) {
		return "systemml_hop_pre_" + hop.getHopID();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("dwhile (");
		sb.append(_predicate);
		sb.append(" @\n");
		sb.append(_dIterBefore.toString());
		sb.append(" @\n");
		sb.append(_dIterAfter.toString());
		sb.append(") { \n");
		for (StatementBlock block : _body) {
			sb.append(block.toString());
		}
		sb.append("}\n");
		return sb.toString();
	}

}