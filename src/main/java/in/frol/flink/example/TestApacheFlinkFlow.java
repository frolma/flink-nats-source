package in.frol.flink.example;

import in.frol.flink.source.nats.NatsSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestApacheFlinkFlow {

    private static final int PLATFORM_RESTART_STRATEGIES_ATTEMPTS = 1_000_000_000;
    private static final long PLATFORM_RESTART_STRATEGIES_DELAY_MS = 400L;

    public static void main(String[] args) throws IOException {
        final TestApacheFlinkFlow app = new TestApacheFlinkFlow();
        final ParameterTool parameterTool = ParameterTool.fromPropertiesFile("flink.properties");
        final Properties properties = parameterTool.getProperties();
        final StreamExecutionEnvironment environment = app.getEnvironment(properties);
        final NatsSourceFunction.NatsParameters natsParameters = new NatsSourceFunction
                .NatsParameters("host", "subject", "queue");
        final NatsSourceFunction natsSource = new NatsSourceFunction(natsParameters);
        environment
                .addSource(natsSource)
                .name("source: receiving data from NATS queue")

                .map(app.transform())
                .name("map: convert to sink comp. object")

                .sinkTo(new PrintSink<>())
                .name("Test Print Sink");
    }

    private StreamExecutionEnvironment getEnvironment(Properties params) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        environment.setRestartStrategy(this.restartStrategy());
        return environment;
    }

    public RestartStrategies.RestartStrategyConfiguration restartStrategy() {
        return RestartStrategies.fixedDelayRestart(PLATFORM_RESTART_STRATEGIES_ATTEMPTS,
                Time.of(PLATFORM_RESTART_STRATEGIES_DELAY_MS, TimeUnit.MILLISECONDS));
    }

    private MapFunction<String, String> transform() {
        return val -> val;
    }
}
