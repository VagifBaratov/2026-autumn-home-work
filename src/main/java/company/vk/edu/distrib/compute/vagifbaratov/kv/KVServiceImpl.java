package company.vk.edu.distrib.compute.vagifbaratov.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.vagifbaratov.kv.replication.ReplicaClientImpl;
import company.vk.edu.distrib.compute.vagifbaratov.kv.replication.ReplicaCoordinator;
import company.vk.edu.distrib.compute.vagifbaratov.kv.replication.ReplicaResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class KVServiceImpl implements KVService {
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String STATUS_PATH = "/v0/status";
    public static final String ID_ARG = "id";
    public static final String ACK_ARG = "ack";
    public static final int MIN_ACK = 1;
    public static final String REPLICA_HEADER = "X-KV-Replica";

    private final int port;
    private final HttpServer server;
    private final ReplicaClientImpl replicaClient;
    private final ReplicaCoordinator coordinator;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        this.port = port;
        replicaClient = new ReplicaClientImpl(port, dao);
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::handle);
        server.setExecutor(executor);
        coordinator = new ReplicaCoordinator(replicaClient);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);

        executor.shutdownNow();
        executor.close();

        coordinator.close();

        try {
            replicaClient.close();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                dispatch(exchange);
            } catch (IllegalArgumentException exception) {
                exchange.sendResponseHeaders(400, -1);
            } catch (NoSuchElementException exception) {
                exchange.sendResponseHeaders(404, -1);
            } catch (IOException exception) {
                exchange.sendResponseHeaders(503, -1);
            }
        }
    }

    private void dispatch(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();
        if (STATUS_PATH.equals(path)) {
            exchange.sendResponseHeaders("GET".equals(method) ? 200 : 503, -1);
            return;
        }
        if (ENTITY_PATH.equals(path)) {
            if (!"GET".equals(method) && !"PUT".equals(method) && !"DELETE".equals(method)) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            handleEntity(exchange);
            return;
        }

        exchange.sendResponseHeaders(404, -1);
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String key = extractArg(exchange, ID_ARG, "");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }

        ClusterConfig cfg = ClusterConfigFactory.load(port);

        String ackText = extractArg(exchange, ACK_ARG, Integer.toString(MIN_ACK));
        int ack;
        try {
            ack = Integer.parseInt(ackText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid ack", e);
        }

        if (ack < MIN_ACK || ack > cfg.replicationFactor()) {
            throw new IllegalArgumentException("Invalid ack");
        }

        String method = exchange.getRequestMethod();

        byte[] body = "PUT".equals(method) ? exchange.getRequestBody().readAllBytes() : new byte[0];
        ReplicaResponse response;

        if ("true".equals(exchange.getRequestHeaders().getFirst(REPLICA_HEADER))) {
            response = replicaClient.localQuery(method, key, body);
        } else {
            response = coordinator.execute(method, key, body, ack, cfg);
        }

        if (response.status() == 200) {
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        }
        exchange.sendResponseHeaders(response.status(), response.body().length == 0 ? -1 : response.body().length);

        if (response.body().length > 0) {
            exchange.getResponseBody().write(response.body());
        }
    }

    private static String extractArg(HttpExchange exchange, String name, String defaultValue) {
        String query = exchange.getRequestURI().getRawQuery();

        if (query != null) {
            for (String parameter : query.split("&")) {
                String[] pair = parameter.split("=", 2);
                String argName = URLDecoder.decode(pair[0], StandardCharsets.UTF_8);

                if (name.equals(argName)) {
                    return pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
                }
            }
        }
        return defaultValue;
    }
}
