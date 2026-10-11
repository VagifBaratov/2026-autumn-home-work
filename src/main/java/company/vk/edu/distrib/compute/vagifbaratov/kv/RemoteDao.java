package company.vk.edu.distrib.compute.vagifbaratov.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

/** UTF-8 adapter for the shortener's string values over the binary KV protocol. */
public final class RemoteDao implements Dao<String> {
    private final HttpClient client = HttpClient.newHttpClient();

    private final String host;
    private final int port;



    public RemoteDao(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public synchronized String get(String key) throws IOException {
        return new String(request("GET", key, new byte[0], 200), StandardCharsets.UTF_8);
    }

    @Override
    public synchronized void upsert(String key, String value) throws IOException {
        request("PUT", key, Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8), 201);
    }

    @Override
    public synchronized void delete(String key) throws IOException {
        request("DELETE", key, new byte[0], 202);
    }

    @Override
    public synchronized void close() {
        client.close();
    }

    private byte[] request(String method, String key, byte[] value, int expectedStatus) throws IOException {

        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must be non-empty");
        }

        var path = host + ":" + port + KVServiceImpl.ENTITY_PATH + KVServiceImpl.ID_ARG + "=";
        var request = HttpRequest.newBuilder().uri(
                URI.create(path + URLEncoder.encode(key, StandardCharsets.UTF_8))
        );

        switch (method) {
            case "GET" -> request.GET();
            case "DELETE" -> request.DELETE();
            default -> request.PUT(HttpRequest.BodyPublishers.ofByteArray(value));

        }

        try {
            var response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
            int status = response.statusCode();

            if (status == 404 && "GET".equals(method)) {
                throw new NoSuchElementException("Missing key: " + key);
            }
            if (status != expectedStatus) {
                throw new IOException("Unexpected KV response: " + status);
            }
            return response.body();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted", exception);
        }
    }
}
