FROM kahing/goofys-bench

RUN apt-get update && apt-get install -y --no-install-recommends sshfs && apt-get clean
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

ENV PATH=$PATH:/root/.cargo/bin
ADD Cargo.lock Cargo.toml /root/catfs/
WORKDIR /root/catfs
RUN mkdir /root/catfs/src && touch /root/catfs/src/lib.rs
# there's no source yet, just build the dependencies
RUN cargo fetch
RUN (cargo build --release || true) && rm /root/catfs/src/lib.rs

ADD . /root/catfs
RUN cargo install && rm -Rf target/

ENTRYPOINT ["/root/catfs/bench/run_bench.sh"]
