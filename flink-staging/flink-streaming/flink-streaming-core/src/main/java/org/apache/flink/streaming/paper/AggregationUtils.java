package org.apache.flink.streaming.paper;

import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.windowing.MultiDiscretizer;
import org.apache.flink.streaming.api.windowing.policy.DeterministicPolicyGroup;
import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.apache.flink.streaming.api.windowing.policy.TriggerPolicy;

@SuppressWarnings("serial")
public class AggregationUtils {

	public static class WindowAggregation<A> {

		MapFunction<Double, A> lift;
		ReduceFunction<A> combine;
		MapFunction<A, Double> lower;

		WindowAggregation(MapFunction<Double, A> lift,
				ReduceFunction<A> combine, MapFunction<A, Double> lower) {
			this.lift = lift;
			this.combine = combine;
			this.lower = lower;
		}

		public DataStream<Double> applyOn(
				DataStream<Tuple2<Double, Double>> input,
				LinkedList<DeterministicPolicyGroup<Tuple2<A, Double>>> deterministicPolicyGroups,
				LinkedList<TriggerPolicy<Tuple2<A, Double>>> notDeterministicTriggerPolicies,
				LinkedList<EvictionPolicy<Tuple2<A, Double>>> notDeterministicEvictionPolicies) {

			TypeInformation<Tuple2<A, Double>> liftReturnType = new TupleTypeInfo<Tuple2<A, Double>>(
					TypeExtractor.getMapReturnTypes(lift,
							BasicTypeInfo.DOUBLE_TYPE_INFO),
					BasicTypeInfo.DOUBLE_TYPE_INFO);

			DataStream<Tuple2<A, Double>> lifted = input.map(new Lift<A>(lift))
					.returns(liftReturnType);

			TypeInformation<Tuple2<Integer, Tuple2<A, Double>>> combinedType = new TupleTypeInfo<Tuple2<Integer, Tuple2<A, Double>>>(
					BasicTypeInfo.INT_TYPE_INFO, liftReturnType);

			DataStream<Tuple2<Integer, Tuple2<A, Double>>> combinedWithID = lifted
					.transform("WindowAggregation", combinedType,
							new MultiDiscretizer<Tuple2<A, Double>>(
									deterministicPolicyGroups,
									notDeterministicTriggerPolicies,
									notDeterministicEvictionPolicies,
									new Combine<A>(combine)));

			DataStream<A> combinedWithoutID = combinedWithID
					.map(new MapFunction<Tuple2<Integer, Tuple2<A, Double>>, A>() {
						private static final long serialVersionUID = 1L;

						@Override
						public A map(Tuple2<Integer, Tuple2<A, Double>> value)
								throws Exception {
							return value.f1.f0;
						}

					});

			return combinedWithoutID.map(lower);
		}
	}
	
	public static WindowAggregation<Double> SumAggregation = new WindowAggregation<Double>(
			new IdMap(), new ReduceFunction<Double>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Double reduce(Double value1, Double value2)
						throws Exception {
					return value1 + value2;
				}
			}, new IdMap());

	public static class IdMap implements MapFunction<Double, Double> {

		@Override
		public Double map(Double value) throws Exception {
			return value;
		}

	}

	public static class Combine<A> implements ReduceFunction<Tuple2<A, Double>> {

		ReduceFunction<A> combine;

		Combine(ReduceFunction<A> combine) {
			this.combine = combine;
		}

		@Override
		public Tuple2<A, Double> reduce(Tuple2<A, Double> value1,
				Tuple2<A, Double> value2) throws Exception {

			value1.f0 = combine.reduce(value1.f0, value2.f0);
			return value1;

		}
	}

	public static class Lift<A> implements
			MapFunction<Tuple2<Double, Double>, Tuple2<A, Double>> {

		private MapFunction<Double, A> lift;

		Lift(MapFunction<Double, A> lift) {
			this.lift = lift;
		}

		@Override
		public Tuple2<A, Double> map(Tuple2<Double, Double> value)
				throws Exception {
			return new Tuple2<A, Double>(lift.map(value.f0), value.f1);
		}

	}

}
