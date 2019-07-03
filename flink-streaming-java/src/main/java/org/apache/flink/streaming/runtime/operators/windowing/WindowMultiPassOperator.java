package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.datastream.IterativeWindowStream;
import org.apache.flink.streaming.api.datastream.SharedLoopContext;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.types.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

@Internal
public class WindowMultiPassOperator<K, IN1, IN2, R, S, W2 extends Window>
	extends AbstractStreamOperator<Either<R, S>>
	implements TwoInputStreamOperator<IN1, IN2, Either<R, S>>, Serializable {

	public final static Logger logger = LoggerFactory.getLogger(WindowMultiPassOperator.class);
	private final KeySelector<IN1, K> entryKeying;
	private final KeySelector<IN2, K> feedbackKeying;

	private WindowOperator<K, IN2, ?, Either<R, S>, W2> superstepWindow;

	//UDF
	WindowLoopFunction loopFunction;

	Set<List<Long>> activeIterations = new HashSet<>();
	StreamTask<?, ?> containingTask;

	TimestampedCollector<Either<R, S>> collector;

	//TODO back these to managed state eventually
	Map<List<Long>, Map<K, List<IN1>>> entryBuffer;
	Map<List<Long>, Set<K>> activeKeys;

	// MY METRICS
	private Map<List<Long>, Long> lastWinStartPerContext = new HashMap<>();
	private Map<List<Long>, Long> lastLocalEndPerContext = new HashMap<>();

	public WindowMultiPassOperator(KeySelector<IN1, K> entryKeySelector, KeySelector<IN2, K> feedbackKeySelector, WindowOperator superstepWindow, WindowLoopFunction loopFunction) {
		this.entryKeying = entryKeySelector;
		this.feedbackKeying = feedbackKeySelector;
		this.superstepWindow = superstepWindow;
		this.loopFunction = loopFunction;
	}

	class InnerLoopContext implements SharedLoopContext<S> {

		private MapState<Long, S> loopState;
		private ValueState<S> persistentState;

		@Override
		public MapState<Long, S> getWindowLoopState(LoopContext loopCtx) {
			if (loopState == null)
				loopState = getRuntimeContext().getMapState(new MapStateDescriptor<>("loopState", TypeInformation.of(Long.class), loopFunction.getStateType()));
			return loopState;
		}

		@Override
		public ValueState<S> getPersistentLoopState(LoopContext loopCtx) {
			if (persistentState == null)
				persistentState = getRuntimeContext().getState(new ValueStateDescriptor<>("persistentState", loopFunction.getStateType()));
			return persistentState;
		}
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Either<R, S>>> output) {
		super.setup(containingTask, config, output);

		// setup() both with own output
		StreamConfig config2 = new StreamConfig(config.getConfiguration().clone());
		config2.setOperatorName("WinOp2");
		superstepWindow.setup(containingTask, config2, output);

		((IterativeWindowStream.StepWindowFunction) ((InternalIterableWindowFunction) superstepWindow.getUserFunction()).getWrappedFunction()).setSharedLoopContext(new InnerLoopContext());

		this.containingTask = containingTask;
		this.entryBuffer = new HashMap<>();
		this.activeKeys = new HashMap<>();
	}

	@Override
	public final void open() throws Exception {
		collector = new TimestampedCollector<>(output);
		superstepWindow.getOperatorConfig().setStateKeySerializer(config.getStateKeySerializer(containingTask.getUserCodeClassLoader()));
		super.open();
		superstepWindow.open();
	}

	@Override
	public final void close() throws Exception {
		super.close();
		superstepWindow.close();
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		superstepWindow.dispose();
	}

	public void processElement1(StreamRecord<IN1> element) throws Exception {
		logger.info(getRuntimeContext().getNumberOfParallelSubtasks() + " - " + getRuntimeContext().getMaxNumberOfParallelSubtasks() + " ::: " + getRuntimeContext().getIndexOfThisSubtask() + ":: TWOWIN Received e from IN - " + element);
		activeIterations.add(element.getProgressContext());

		if (!entryBuffer.containsKey(element.getProgressContext())) {
			entryBuffer.put(element.getProgressContext(), new HashMap<K, List<IN1>>());
		}

		Map<K, List<IN1>> tmp = entryBuffer.get(element.getProgressContext());
		K key = entryKeying.getKey(element.getValue());
		if (!tmp.containsKey(key)) {
			tmp.put(key, new ArrayList<IN1>());
		}
		tmp.get(key).add(element.getValue());
	}

	public void processElement2(StreamRecord<IN2> element) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() + ":: TWOWIN Received from FEEDBACK - " + element);
		superstepWindow.setCurrentKey(feedbackKeying.getKey(element.getValue()));
		if (activeIterations.contains(element.getProgressContext())) {
			superstepWindow.processElement(element);
		}
	}

	//TODO add latch for consistent watermark processing 
	public void processWatermark1(Watermark mark) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() + ":: TWOWIN Received from IN - " + mark);
		lastWinStartPerContext.put(mark.getContext(), System.currentTimeMillis());
		if (entryBuffer.containsKey(mark.getContext())) {
			for (Map.Entry<K, List<IN1>> entry : entryBuffer.get(mark.getContext()).entrySet()) {
				collector.setAbsoluteTimestamp(mark.getContext(), 0);
				loopFunction.entry(new LoopContext(mark.getContext(), 0, entry.getKey(), getRuntimeContext()), entry.getValue(), collector);
			}
			activeKeys.put(mark.getContext(), entryBuffer.get(mark.getContext()).keySet());
			entryBuffer.remove(mark.getContext()); //entry is done for that context
		}
		output.emitWatermark(mark);
		lastLocalEndPerContext.put(mark.getContext(), System.currentTimeMillis());
	}

	//TODO what happens when no accessible context exists in that partition
	public void processWatermark2(Watermark mark) throws Exception {
		logger.info(getRuntimeContext().getIndexOfThisSubtask() + ":: TWOWIN Received from FEEDBACK - " + mark);
		lastWinStartPerContext.put(mark.getContext(), System.currentTimeMillis());
		if (mark.iterationDone()) {
			activeIterations.remove(mark.getContext());
			if (mark.getContext().get(mark.getContext().size() - 1) != Long.MAX_VALUE) {
				//TODO trigger per key and cleanup
				loopFunction.onTermination(new LoopContext(mark.getContext(), mark.getTimestamp(), null, getRuntimeContext()), collector);
			}
			superstepWindow.processWatermark(new Watermark(mark.getContext(), Long.MAX_VALUE, true, mark.iterationOnly()));
		} else {
			superstepWindow.processWatermark(mark);
		}
		lastLocalEndPerContext.put(mark.getContext(), System.currentTimeMillis());
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		superstepWindow.initializeState();
		//TODO initialize state
	}

	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
	}

	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
	}

	@Override
	public void sendMetrics(long windowEnd, List<Long> context) {
	}
}
