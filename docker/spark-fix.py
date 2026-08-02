content = open('docker-compose.yml').read()

old_master = """  spark-master:
    image: bitnami/spark:latest
    container_name: gitpulse-spark-master
    restart: unless-stopped
    environment:
      SPARK_MODE: master
      SPARK_RPC_AUTHENTICATION_ENABLED: "no"
      SPARK_RPC_ENCRYPTION_ENABLED: "no"
      SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED: "no"
      SPARK_SSL_ENABLED: "no"
    volumes:
      - ../spark_jobs:/opt/spark_jobs
    ports:
      - "7077:7077"
      - "8081:8080"
    networks:
      - gitpulse-net
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 15s
      timeout: 10s
      retries: 5"""

new_master = """  spark-master:
    image: apache/spark:3.5.0
    container_name: gitpulse-spark-master
    restart: unless-stopped
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    environment:
      SPARK_NO_DAEMONIZE: "true"
    volumes:
      - ../spark_jobs:/opt/spark_jobs
    ports:
      - "7077:7077"
      - "8081:8080"
    networks:
      - gitpulse-net
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 15s
      timeout: 10s
      retries: 5"""

old_worker = """  spark-worker:
    image: bitnami/spark:latest
    container_name: gitpulse-spark-worker
    restart: unless-stopped
    depends_on:
      spark-master:
        condition: service_healthy
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G
      SPARK_WORKER_CORES: 2
      SPARK_RPC_AUTHENTICATION_ENABLED: "no"
      SPARK_RPC_ENCRYPTION_ENABLED: "no"
    volumes:
      - ../spark_jobs:/opt/spark_jobs
    ports:
      - "8082:8081"
    networks:
      - gitpulse-net"""

new_worker = """  spark-worker:
    image: apache/spark:3.5.0
    container_name: gitpulse-spark-worker
    restart: unless-stopped
    depends_on:
      spark-master:
        condition: service_healthy
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    environment:
      SPARK_NO_DAEMONIZE: "true"
      SPARK_WORKER_MEMORY: 2g
      SPARK_WORKER_CORES: "2"
    volumes:
      - ../spark_jobs:/opt/spark_jobs
    ports:
      - "8082:8081"
    networks:
      - gitpulse-net"""

content = content.replace(old_master, new_master)
content = content.replace(old_worker, new_worker)
open('docker-compose.yml', 'w').write(content)
print("Done!")
