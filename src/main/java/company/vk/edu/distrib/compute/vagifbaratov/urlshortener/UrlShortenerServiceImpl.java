package company.vk.edu.distrib.compute.vagifbaratov.urlshortener;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vagifbaratov.urlshortener.handler.*;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.Objects;

public class UrlShortenerServiceImpl implements UrlShortenerService {
    private static final Logger log = LoggerFactory.getLogger(UrlShortenerServiceImpl.class);

    private final HttpServer server;
    private Dao<String> linksDao;
    private final Dao<String> credentialsDao;
    private boolean started;
    private boolean stopped;

    public UrlShortenerServiceImpl(int port, Dao<String> linksDao, Dao<String> credentialsDao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.linksDao = linksDao;
        this.credentialsDao = credentialsDao;

        server.createContext(
                "/v0/status",
                new ErrorHandler(new StatusHandler()));

        server.createContext(
                "/v0/links",
                new ErrorHandler(
                        new BasicAuthHandler(
                                new LinksHandler(port, linksDao),
                                credentialsDao
                        )
                )
        );

        server.createContext(
                "/internal/users",
                new ErrorHandler(new InternalHandler(credentialsDao))
        );

        server.createContext("/", new ErrorHandler(new RedirectHandler(linksDao)));
    }

    @Override
    public synchronized void setLinksDao(Dao<String> dao) {
        if (started || stopped) {
            throw new IllegalStateException("Cannot replace DAO after start or stop");
        }
        Objects.requireNonNull(dao, "dao");
        if (linksDao == dao) {
            return;
        }
        try {
            linksDao.close();
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
        linksDao = dao;
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service was already started");
        }

        server.start();
        started = true; stopped = false;
    }

    @Override
    public void stop() {
        if (stopped) {
            throw new IllegalStateException("Service was already stopped");
        }
        server.stop(1);
        stopped = true; started = false;
        try {
            linksDao.close();
        } catch (IOException e) {
            log.error("Closing linksDao resulted in error", e);
        }

        try {
            credentialsDao.close();
        } catch (IOException e) {
            log.error("Closing credentialsDao resulted in error", e);
        }
    }
}
