package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.Function2;
import scala.Tuple3;

public class CountReduceFunction implements Function2<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>,
		Tuple3<Integer, Integer, Long>> {

	private static final long serialVersionUID = 1886318890063064287L;

	@Override
	public Tuple3<Integer, Integer, Long> call(Tuple3<Integer, Integer, Long> t1,
										 Tuple3<Integer, Integer, Long> t2) {
		return new Tuple3<>(t1._1() + t2._1(), t1._2() + t2._2(), t1._3() + t2._3());
	}

}
