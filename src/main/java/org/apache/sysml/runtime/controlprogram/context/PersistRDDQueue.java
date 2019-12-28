package org.apache.sysml.runtime.controlprogram.context;

import org.apache.spark.api.java.JavaPairRDD;

public class PersistRDDQueue {

	private int _size = 3;

	private JavaPairRDD[] _rddHandles = new JavaPairRDD[_size];

	public PersistRDDQueue() {
	}

	public PersistRDDQueue(int size) {
		_size = size;
	}

	// 表示最新值的位置
	private int _flag = 0;

	/**
	 * 添加 rddHandle. 会替换旧值
	 */
	public void add(JavaPairRDD rddHandle) {
		_flag = (_flag + 1) % _size;
		_rddHandles[_flag] = rddHandle;
	}

	/**
	 * @return 旧的 rddHandle
	 */
	public JavaPairRDD getOld() {
		return _rddHandles[(_flag + 1) % _size];
	}

	/**
	 * @return 新的 rddHandle
	 */
	public JavaPairRDD getNew () {
		return _rddHandles[_flag % _size];
	}

}
