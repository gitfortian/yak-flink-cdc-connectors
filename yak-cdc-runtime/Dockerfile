FROM eclipse-temurin:11-jre-jammy

ARG FLINK_VERSION=1.20.5
ARG FLINK_CDC_VERSION=3.6.0
ARG MYSQL_DRIVER_VERSION=8.4.0
ARG POSTGRES_DRIVER_VERSION=42.7.7
ARG FLINK_SHA512
ARG FLINK_CDC_SHA512
ENV FLINK_HOME=/opt/flink FLINK_CDC_HOME=/opt/flink-cdc PATH=/opt/flink/bin:/opt/flink-cdc/bin:$PATH

RUN test -n "$FLINK_SHA512" && test -n "$FLINK_CDC_SHA512" && apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/* && \
    curl -fsSLo /tmp/flink.tgz "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" && \
    echo "$FLINK_SHA512  /tmp/flink.tgz" | sha512sum -c - && tar -xzf /tmp/flink.tgz -C /opt && mv "/opt/flink-${FLINK_VERSION}" "$FLINK_HOME" && \
    curl -fsSLo /tmp/cdc.tgz "https://archive.apache.org/dist/flink/flink-cdc-${FLINK_CDC_VERSION}/flink-cdc-${FLINK_CDC_VERSION}-bin.tar.gz" && \
    echo "$FLINK_CDC_SHA512  /tmp/cdc.tgz" | sha512sum -c - && mkdir "$FLINK_CDC_HOME" && tar -xzf /tmp/cdc.tgz -C "$FLINK_CDC_HOME" --strip-components=1 && \
    curl -fsSLo "$FLINK_CDC_HOME/lib/mysql-connector-j.jar" "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar" && \
    curl -fsSLo "$FLINK_CDC_HOME/lib/postgresql.jar" "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar" && rm /tmp/*.tgz

COPY yak-flink-cdc-connector-jdbc/target/yak-flink-cdc-connector-jdbc-0.1.0-SNAPSHOT.jar /opt/flink-cdc/lib/yak-jdbc.jar
COPY yak-cdc-runtime/runtime-manifest.json /opt/yak/runtime-manifest.json
COPY yak-cdc-runtime/src/Gateway.java /opt/yak/Gateway.java
RUN javac --release 11 -d /opt/yak /opt/yak/Gateway.java && rm /opt/yak/Gateway.java && useradd --system --uid 10001 --home /opt/yak yak && mkdir -p /run/yak && chown -R yak /run/yak /opt/flink /opt/flink-cdc
USER 10001
EXPOSE 8080
ENTRYPOINT ["java", "-cp", "/opt/yak", "Gateway"]
