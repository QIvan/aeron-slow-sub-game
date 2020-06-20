package org.mysun;

import com.sun.net.httpserver.HttpServer;
import io.aeron.samples.CncFileReader;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.status.CountersReader;

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
      counters.forEach((counterId, typeId, keyBuffer, label) -> {
            long value = counters.getCounterValue(counterId);
            var counter = registry.counter("aeron", "label", label);
            counter.increment(value - counter.count());
          }
      );
    }, 0, 1, TimeUnit.SECONDS);

    new Thread(server::start)
        .start();
  }
}
