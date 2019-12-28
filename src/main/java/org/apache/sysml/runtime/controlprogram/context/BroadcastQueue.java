package org.apache.sysml.runtime.controlprogram.context;

import org.apache.spark.broadcast.Broadcast;

public class BroadcastQueue {

	private int _size = 3;

	private Broadcast[] _rddHandles = new Broadcast[_size];

	// 表示最新值的位置
	private int _flag = 0;

	/**
	 * 添加 rddHandle. 会替换旧值
	 */
	public void add(Broadcast rddHandle) {
		_flag = (_flag + 1) % _size;
		_rddHandles[_flag] = rddHandle;
	}

	/**
	 * @return 旧的 rddHandle
	 */
	public Broadcast getOld() {
		return _rddHandles[(_flag + 1) % _size];
	}

}
