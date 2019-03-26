package org.apache.sysml.parser;

import org.apache.sysml.hops.Hop;

public class PreDataIdentifier extends DataIdentifier {

	private String _originName;

	public PreDataIdentifier(String name, String originName) {
		super(name);
		_originName = originName;
	}

	public PreDataIdentifier(Hop hop) {
		_name = DWhileStatement.getPreOutputNameFromHop(hop);
		_originName = hop.getName();
		_dataType = hop.getDataType();
		_valueType = hop.getValueType();
		_dim1 = hop.getDim1();
		_dim2 = hop.getDim2();
		_rows_in_block = hop.getRowsInBlock();
		_columns_in_block = hop.getColsInBlock();
		_nnz = hop.getNnz();
	}

	public String getOriginName() {
		return _originName;
	}

	public void setOriginName(String originName) {
		_originName = originName;
	}

}
