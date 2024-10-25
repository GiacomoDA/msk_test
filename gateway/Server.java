package it.polimi.gateway;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.Properties;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import com.sun.net.httpserver.*;
import java.util.HashMap;
import java.util.Map;

public class Server {

    public static KafkaService kafkaService = new KafkaService();
    public static int portNumber;
    public static int timeout;

    public static void main(String[] args) {

        HttpServer server;
        try {
            Properties properties = new Properties();
            FileInputStream input = new FileInputStream("config.properties");
            properties.load(input);

            portNumber = Integer.valueOf(properties.getProperty("server.port", "8000"));
            timeout = Integer.valueOf(properties.getProperty("server.timeout", "30"));

            server = HttpServer.create(new InetSocketAddress(portNumber), 0);
            server.createContext("/", new RequestHandler());
            server.setExecutor(null);
            server.start();
            Logger.Log("Server started on port " + portNumber);
        } catch (IOException e) {
            Logger.Log("Error starting server");
            e.printStackTrace();
        }
    }

    static class RequestHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("GET")) {
                sendResponse(exchange, 405, "Method not allowed");
                return;
            }

            String query = exchange.getRequestURI().getQuery();
            Map<String, String> parameters = getQueryParameters(query);

            if (!parameters.keySet().contains("topic") || !parameters.keySet().contains("value")) {
                sendResponse(exchange, 400, "Missing required parameters");
                return;
            }

            Logger.Log("Received request - topic: " + parameters.get("topic") + " value: " + parameters.get("value"));

            String requestId = UUID.randomUUID().toString();

            CompletableFuture<String> responseFuture = kafkaService.publishEvent(requestId, parameters.get("topic"), parameters.get("value"));

            responseFuture.orTimeout(timeout, java.util.concurrent.TimeUnit.SECONDS)
                .thenApply(responseData -> sendResponse(exchange, 200, responseData))
                .exceptionally(e -> sendResponse(exchange, 504, "Timeout occurred"));
        }

        private Void sendResponse(HttpExchange exchange, int statusCode, String response) {
            try {
                exchange.sendResponseHeaders(statusCode, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return null;
        }

        // simple query splitting, does not work with multiple values per parameter
        private Map<String, String> getQueryParameters(String query) {
            String[] parameters = query.split("&");
            Map<String, String> map = new HashMap<String, String>();
            for (String parameter : parameters) {
                String key = parameter.split("=")[0];
                String value = parameter.split("=")[1];
                map.put(key, value);
            }
            return map;
        }
    }
}