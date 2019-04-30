package org.apache.sysml.runtime.controlprogram.context;

import org.apache.spark.api.java.JavaPairRDD;

public class PersistRDDQueue {

	private JavaPairRDD[] _rddHandles = new JavaPairRDD[2];

	// true 表示最新值的位置是0; false 表示最新值的位置是1
	private boolean _flag = true;

	/**
	 * 添加 rddHandle. 会替换旧值
	 */
	public void add(JavaPairRDD rddHandle) {
		if (_flag) {
			_rddHandles[1] = rddHandle;
		} else {
			_rddHandles[0] = rddHandle;
		}
		_flag = !_flag;
	}

	/**
	 * @return 旧的 rddHandle
	 */
	public JavaPairRDD getOld() {
		return _flag ? _rddHandles[1] : _rddHandles[0];
	}

	/**
	 * @return 新的 rddHandle
	 */
	public JavaPairRDD getNew() {
		return _flag ? _rddHandles[0] : _rddHandles[1];
	}

}
