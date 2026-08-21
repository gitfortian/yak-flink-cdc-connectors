import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.UUID;
import java.util.concurrent.Executors;

/** Phase-0 single-runtime gateway. It intentionally has no upload or plugin-install endpoint. */
public final class Gateway {
  private static final Path RUN = Paths.get("/run/yak");
  private static Process job;
  private static String jobId;

  public static void main(String[] args) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
    server.createContext("/health", x -> json(x, 200, "{\"status\":\"ok\"}"));
    server.createContext("/capabilities", Gateway::capabilities);
    server.createContext("/validate", x -> validate(x, false));
    server.createContext("/deploy", x -> validate(x, true));
    server.createContext("/status", Gateway::status);
    server.createContext("/stop", Gateway::stop);
    server.createContext("/logs", Gateway::logs);
    server.setExecutor(Executors.newFixedThreadPool(4));
    server.start();
  }

  private static void capabilities(HttpExchange x) throws IOException {
    require(x, "GET");
    json(x, 200, Files.readString(Paths.get("/opt/yak/runtime-manifest.json")));
  }

  private static synchronized void validate(HttpExchange x, boolean deploy) throws IOException {
    require(x, "POST");
    String yaml = new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    String lower = yaml.toLowerCase();
    if (yaml.length() > 262144 || !lower.contains("type: mysql") || !lower.contains("type: yak-jdbc")) {
      json(x, 422, "{\"valid\":false,\"error\":\"only mysql to yak-jdbc pipeline YAML is allowed\"}"); return;
    }
    if (lower.matches("(?s).*password\\s*:\\s*(?!\\$\\{env:)[^\\r\\n]+.*")) {
      json(x, 422, "{\"valid\":false,\"error\":\"credentials must use ${ENV:NAME} references\"}"); return;
    }
    if (!deploy) { json(x, 200, "{\"valid\":true,\"deliverySemantics\":\"at-least-once\"}"); return; }
    if (job != null && job.isAlive()) { json(x, 409, "{\"error\":\"a job is already active\"}"); return; }
    jobId = UUID.randomUUID().toString();
    Path definition = RUN.resolve("pipeline-" + jobId + ".yaml");
    Path log = RUN.resolve("job-" + jobId + ".log");
    // Persist references only. Flink's configuration layer resolves ENV references; resolved
    // credential values never enter the gateway's files, process arguments, responses, or logs.
    Files.writeString(definition, yaml, StandardOpenOption.CREATE_NEW);
    ProcessBuilder pb = new ProcessBuilder("/opt/flink-cdc/bin/flink-cdc.sh", definition.toString());
    pb.redirectErrorStream(true).redirectOutput(log.toFile());
    job = pb.start();
    json(x, 202, "{\"jobId\":\"" + jobId + "\",\"deliverySemantics\":\"at-least-once\"}");
  }

  private static synchronized void status(HttpExchange x) throws IOException {
    require(x, "GET");
    String state = job == null ? "NONE" : job.isAlive() ? "RUNNING" : "TERMINATED";
    json(x, 200, "{\"jobId\":" + (jobId == null ? "null" : "\""+jobId+"\"") + ",\"status\":\""+state+"\"}");
  }
  private static synchronized void stop(HttpExchange x) throws IOException {
    require(x, "POST");
    if (job != null && job.isAlive()) job.destroy();
    if (jobId != null) Files.deleteIfExists(RUN.resolve("pipeline-" + jobId + ".yaml"));
    json(x, 200, "{\"status\":\"stopping\"}");
  }
  private static synchronized void logs(HttpExchange x) throws IOException {
    require(x, "GET");
    if (jobId == null) { json(x, 404, "{\"error\":\"no job\"}"); return; }
    Path log = RUN.resolve("job-" + jobId + ".log");
    String value = Files.exists(log) ? Files.readString(log) : "";
    value = value.replaceAll("(?i)(password\\s*[=:]\\s*)\\S+", "$1***");
    json(x, 200, "{\"logs\":\"" + escape(value) + "\"}");
  }
  private static void require(HttpExchange x, String method) throws IOException {
    if (!x.getRequestMethod().equals(method)) { json(x, 405, "{\"error\":\"method not allowed\"}"); throw new IOException("method"); }
  }
  private static void json(HttpExchange x, int status, String body) throws IOException {
    byte[] bytes=body.getBytes(StandardCharsets.UTF_8); x.getResponseHeaders().set("Content-Type", "application/json"); x.sendResponseHeaders(status, bytes.length); x.getResponseBody().write(bytes); x.close();
  }
  private static String escape(String s) { return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", ""); }
}
