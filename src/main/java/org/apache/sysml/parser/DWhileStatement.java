package org.apache.sysml.parser;

// TODO added by czh
public class DWhileStatement extends WhileStatement {

	private PrintStatement _dIterBegin;

	// TODO added by czh
	private ParseInfo _dIterAfter;

	public PrintStatement getDIterBegin() {
		return _dIterBegin;
	}

	public void setDIterBegin(PrintStatement dIterBegin) {
		_dIterBegin = dIterBegin;
	}

	public String getDVarPreName(DataIdentifier dVar) {
		return "systemml_pre_" + dVar.getName();
	}

	public String getDVarDName(DataIdentifier dVar) {
		return "systemml_d_" + dVar.getName();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("dwhile (");
		sb.append(_predicate);
		sb.append(" @\n");
		sb.append(_dIterBegin.toString());
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
