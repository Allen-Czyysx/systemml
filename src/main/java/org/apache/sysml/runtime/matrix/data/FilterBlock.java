package org.apache.sysml.runtime.matrix.data;

import java.io.Serializable;

public class FilterBlock implements Serializable {

	private static final long serialVersionUID = 7446723684649816489L;

	private int[] _data = null;

	public FilterBlock() {
	}

	public FilterBlock(int data) {
		_data = new int[]{data};
	}

	public FilterBlock(int[] data) {
		setData(data);
	}

	public void initData(int length) {
		_data = new int[length];
	}

	public int[] getData() {
		return _data;
	}

	public void setData(int[] data) {
		if (data == null) {
			_data = null;
		} else {
			_data = data;
		}
	}

	public int getData(int i) {
		try {
			return _data[i];
		} catch (Exception e) {
			return 0;
		}
	}

	public void setData(int data) {
		_data = new int[]{data};
	}

	public void setData(int i, int data) {
		_data[i] = data;
	}

	public int getDataNum() {
		if (_data != null) {
			return _data.length;
		} else {
			return 0;
		}
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

}
