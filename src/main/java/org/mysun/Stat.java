package org.mysun;

import com.sun.net.httpserver.HttpServer;
import io.aeron.samples.CncFileReader;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.status.CountersReader;
import org.mysun.BacklogStat.Receiver;
import org.mysun.BacklogStat.Sender;
import org.mysun.BacklogStat.StreamBacklog;
import org.mysun.BacklogStat.StreamCompositeKey;

public class Stat {

  public static void main(String[] args) throws IOException {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var threadPool = Executors.newScheduledThreadPool(1);

    var server = HttpServer.create(new InetSocketAddress(9091), 0);
    server.createContext("/metrics", httpExchange -> {
      String response = registry.scrape();
      httpExchange.sendResponseHeaders(200, response.getBytes().length);
      try (var outputStream = httpExchange.getResponseBody()) {
        outputStream.write(response.getBytes());
      }
    });

    var cncFileReader = CncFileReader.map();

    threadPool.scheduleAtFixedRate(() -> {
      CountersReader counters = cncFileReader.countersReader();

      Map<StreamCompositeKey, StreamBacklog> backLogStatSnapshot = new BacklogStat(counters).snapshot();

      for (var entry : backLogStatSnapshot.entrySet()) {
        StreamCompositeKey key = entry.getKey();
        StreamBacklog streamBacklog = entry.getValue();

        String channel = key.channel();
        int sessionId = key.sessionId();
        int streamId = key.streamId();

        BacklogStat.Publisher publisher = streamBacklog.publisher();
        if (publisher != null) {
          var tags = Tags.of(
              Tag.of("channel", channel),
              Tag.of("session_id", String.valueOf(sessionId)),
              Tag.of("stream_id", String.valueOf(streamId)),
              Tag.of("registration_id", String.valueOf(publisher.registrationId()))
          );
          var positionId = new Id("aeron.publisher.position", tags, "some", "", Type.GAUGE);
          registry.remove(positionId);
          registry.gauge(positionId.getName(), positionId.getTags(), publisher.position());

          var remainingWindowId = positionId.withName("aeron.publisher.remaining_window");
          registry.remove(remainingWindowId);
          registry.gauge(remainingWindowId.getName(), remainingWindowId.getTags(),
              publisher.remainingWindow());

          var limitId = positionId.withName("aeron.publisher.limit");
          registry.remove(limitId);
          registry.gauge(limitId.getName(), limitId.getTags(), publisher.limit());

        }

        Sender sender = streamBacklog.sender();
        if (sender != null) {
          var tags = Tags.of(
              Tag.of("channel", channel),
              Tag.of("session_id", String.valueOf(sessionId)),
              Tag.of("stream_id", String.valueOf(streamId)),
              Tag.of("registration_id", String.valueOf(sender.registrationId()))
          );
          var positionId = new Id("aeron.sender.position", tags, "some", "", Type.GAUGE);
          registry.remove(positionId);
          registry.gauge(positionId.getName(), positionId.getTags(), sender.position());

          var windowId = positionId.withName("aeron.sender.window");
          registry.remove(windowId);
          registry.gauge(windowId.getName(), windowId.getTags(), sender.window());
        }

        Receiver receiver = streamBacklog.receiver();
        if (receiver != null) {
          var tags = Tags.of(
              Tag.of("channel", channel),
              Tag.of("session_id", String.valueOf(sessionId)),
              Tag.of("stream_id", String.valueOf(streamId)),
              Tag.of("registration_id", String.valueOf(receiver.registrationId()))
          );

          var positionId = new Id("aeron.receiver.position", tags, "some", "", Type.GAUGE);
          registry.remove(positionId);
          registry.gauge(positionId.getName(), positionId.getTags(), receiver.position());

          var highWaterMarkId = positionId.withName("aeron.receiver.high_water_mark");
          registry.remove(highWaterMarkId);
          registry.gauge(highWaterMarkId.getName(), highWaterMarkId.getTags(),
              receiver.highWaterMark());
        }

        for (var backlogEntry : streamBacklog.subscriberBacklogs().entrySet()) {
          Long subscriberId = backlogEntry.getKey();
          BacklogStat.Subscriber subscriber = backlogEntry.getValue();
          var tags = Tags.of(
              Tag.of("channel", channel),
              Tag.of("session_id", String.valueOf(sessionId)),
              Tag.of("stream_id", String.valueOf(streamId)),
              Tag.of("registration_id", String.valueOf(subscriberId))
          );

          var backlogId = new Id("aeron.subscriber.backlog", tags, "some", "", Type.GAUGE);
          registry.remove(backlogId);
          registry.gauge(backlogId.getName(), backlogId.getTags(),
              subscriber.backlog(streamBacklog.receiver().highWaterMark()));
        }
      }

//      counters.forEach((counterId, typeId, keyBuffer, label) -> {
//            long value = counters.getCounterValue(counterId);
//            var counter = registry.counter("aeron", "label", label);
//            counter.increment(value - counter.count());
//          }
//      );
    }, 0, 1, TimeUnit.SECONDS);

    new Thread(server::start)
        .start();
  }
}
