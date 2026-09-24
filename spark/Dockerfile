# Note: pin bookworm — Debian trixie (python:3.11-slim default) lacks openjdk-17.
# Spark 3.5 + bundled Arrow 12 require JDK <= 17 (JDK 21 removed DirectByteBuffer(long,int)).
FROM python:3.11-slim-bookworm

ENV SPARK_VERSION=3.5.9 \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    POSTGRES_JDBC=org.postgresql:postgresql:42.7.4 \
    SPARK_KAFKA=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs/ ./jobs/

CMD ["/bin/sh", "-c", \
    "exec /usr/local/bin/spark-submit \
    --master ${SPARK_MASTER:-local[2]} \
    --packages ${SPARK_KAFKA},${POSTGRES_JDBC} \
    --conf spark.driver.memory=768m \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.ui.enabled=false \
    --conf 'spark.driver.extraJavaOptions=-Dio.netty.tryReflectionSetAccessible=true --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/sun.misc=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED' \
    jobs/stream.py"]