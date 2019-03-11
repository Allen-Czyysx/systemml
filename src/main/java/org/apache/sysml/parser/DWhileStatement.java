/*
 * TODO added by czh
 */

package org.apache.sysml.parser;

public class DWhileStatement extends WhileStatement {

	private DataIdentifier _dVar;

	public DWhileStatement() {
		_dVar = null;
	}

	public void setDVar(DataIdentifier dVar) {
		_dVar = dVar;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("dwhile (");
		sb.append(_predicate);
		sb.append(" @ ");
		sb.append(_dVar);
		sb.append(") { \n");
		for (StatementBlock block : _body) {
			sb.append(block.toString());
		}
		sb.append("}\n");
		return sb.toString();
	}

}
