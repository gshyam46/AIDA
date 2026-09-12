FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends git cmake build-essential ca-certificates && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch b10809 https://github.com/ggml-org/llama.cpp.git /src
RUN cmake -S /src -B /src/build -DCMAKE_BUILD_TYPE=Release -DGGML_NATIVE=OFF -DBUILD_SHARED_LIBS=OFF -DLLAMA_CURL=OFF && cmake --build /src/build --target llama-server -j 2
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 curl && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 --create-home aida
COPY --from=build /src/build/bin/llama-server /usr/local/bin/llama-server
USER aida
HEALTHCHECK --interval=10s --timeout=3s --start-period=180s CMD curl --fail --silent http://127.0.0.1:8081/health || exit 1
ENTRYPOINT ["llama-server"]
CMD ["-m", "/models/semantic.gguf", "--alias", "aida-semantic", "--host", "127.0.0.1", "--port", "8081", "-c", "4096", "-np", "1", "-t", "4", "-ngl", "0", "--cache-ram", "0", "--no-webui", "--no-slots", "--log-disable"]
