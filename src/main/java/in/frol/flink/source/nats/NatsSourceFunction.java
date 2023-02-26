package in.frol.flink.source.nats;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class NatsSourceFunction implements SourceFunction<String> {

    private final String hostUrl;
    private final String inboundSubject;
    private final String queue;

    private volatile boolean isRunning;

    public NatsSourceFunction(NatsParameters natsParams) {
        this.hostUrl = natsParams.hostUrl();
        this.inboundSubject = natsParams.inboundSubject();
        this.queue = natsParams.queue();
        this.isRunning = false;
    }

    @Override
    public void run(SourceContext<String> ctx) throws IOException, InterruptedException {
        isRunning = true;

        if (isRunning) {
            try (Connection connection = Nats.connect(hostUrl)) {
                final Queue<String> natsMessagesQueue = new LinkedBlockingQueue<>();

                final Dispatcher dispatcher = connection.createDispatcher((message) -> {
                    final byte[] data = message.getData();
                    final String payload = new String(data, StandardCharsets.UTF_8);
                    natsMessagesQueue.offer(payload);
                });

                dispatcher.subscribe(inboundSubject, queue);

                while (isRunning) {
                    final String message = natsMessagesQueue.poll();
                    if (message != null) {
                        ctx.collect(message);
                    }
                }

                dispatcher.unsubscribe(inboundSubject);

                String message;
                do {
                    message = natsMessagesQueue.poll();
                    if (message != null) {
                        ctx.collect(message);
                    }
                } while (message != null);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public record NatsParameters(
            String hostUrl,
            String inboundSubject,
            String queue) {
    }
}
