package org.apache.sysml.runtime.matrix.data;

import org.apache.sysml.runtime.DMLRuntimeException;

import java.io.Serializable;

public class FilterBlock implements Serializable, Cloneable {

	private static final long serialVersionUID = 7446723684649816489L;

	private int[] _data = null;

	private int _rowNum = -1;

	private int _colNum = -1;

	public FilterBlock() {
	}

	public FilterBlock(int rowNum, int colNum) {
		_rowNum = rowNum;
		_colNum = colNum;
	}

	public FilterBlock(int rowNum, int colNum, int[] data) {
		setData(rowNum, colNum, data);
	}

	public void initData() {
		if (_rowNum == -1 || _colNum == -1) {
			throw new DMLRuntimeException("rowNum = " + _rowNum + ", colNum = " + _colNum);
		}
		_data = new int[_rowNum * _colNum];
	}

	public void initData(int rowNum, int colNum) {
		_rowNum = rowNum;
		_colNum = colNum;
		initData();
	}

	public void setDim(int rowNum, int colNum) {
		_rowNum = rowNum;
		_colNum = colNum;
	}

	public int[] getData() {
		return _data;
	}

	public int getData(int i, int j) {
		checkDim(i, j);
		return _data[i * _colNum + j];
	}

	public void setData(int data) {
		_data = new int[]{data};
		_rowNum = 1;
		_colNum = 1;
	}

	public void setData(int i, int j, int data) {
		checkDim(i, j);
		_data[i * _colNum + j] = data;
	}

	public void setData(int rowNum, int colNum, int[] data) {
		if (data == null) {
			_data = null;
		} else if (data.length <= rowNum * (colNum - 1) || data.length > rowNum * colNum) {
			throw new DMLRuntimeException("rowNum = " + rowNum + ", colNum = " + colNum + ". Cannot fit data.length = "
					+ data.length);
		} else {
			_data = data;
			this._rowNum = rowNum;
			this._colNum = colNum;
		}
	}

	public int getRowNum() {
		return _rowNum;
	}

	public int getColNum() {
		return _colNum;
	}

	public boolean isEmpty() {
		return _data == null || _data.length == 0;
	}

	public int countNonZeros() {
		if (isEmpty()) {
			return 0;
		}

		int ret = 0;
		for (int data : _data) {
			ret += data != 0 ? 1 : 0;
		}
		return ret;
	}

	@Override
	protected FilterBlock clone() throws CloneNotSupportedException {
		FilterBlock ret = (FilterBlock) super.clone();
		ret.setData(_rowNum, _colNum, _data.clone());
		return ret;
	}

	private void checkDim(int r, int c) {
		if (_rowNum == -1 || _colNum == -1) {
			throw new DMLRuntimeException("rowNum or colNum is unknown.");
		}
		if (r >= _rowNum || c >= _colNum || r * _colNum + c > _data.length) {
			throw new DMLRuntimeException("Out of index when reading filterBlock. r = " + r + ", c = " + c
					+ ", rowNum = " + _rowNum + ", colNum = " + _colNum);
		}
	}

}
