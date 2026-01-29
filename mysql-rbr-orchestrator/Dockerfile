# --- Stage 1: Dependency Collector ---
FROM ubuntu:22.04 AS collector
RUN apt-get update && apt-get install -y libjemalloc2 libatomic1 libnuma1 libstdc++6
WORKDIR /lib-collect
RUN cp /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 . && \
    cp /usr/lib/x86_64-linux-gnu/libatomic.so.1 . && \
    cp /usr/lib/x86_64-linux-gnu/libnuma.so.1 . && \
    cp /usr/lib/x86_64-linux-gnu/libstdc++.so.6 .

# --- Stage 2: Final MySQL Image ---
FROM mysql:8.0.43

# Copy libraries
COPY --from=collector /lib-collect/* /usr/lib64/
COPY ./ha_lineairdb_storage_engine.so /usr/lib64/mysql/plugin/
RUN chmod 755 /usr/lib64/mysql/plugin/ha_lineairdb_storage_engine.so

# FIX: Force load jemalloc at startup to avoid TLS block error
ENV LD_PRELOAD=/usr/lib64/libjemalloc.so.2

# Auto-load the plugin
RUN echo "[mysqld]\nplugin-load-add=LineairDB=ha_lineairdb_storage_engine.so" > /etc/mysql/conf.d/lineairdb.cnf
