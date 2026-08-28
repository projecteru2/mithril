FROM rust:1.98-alpine AS build

RUN apk add --no-cache git make musl-dev
WORKDIR /src
COPY . .
RUN make build && ./target/release/mithril --version

FROM alpine:3.22

RUN mkdir -p /etc/mithril
COPY --from=build /src/target/release/mithril /usr/bin/mithril
COPY --from=build /src/mithril.conf.sample /etc/mithril/mithril.conf.sample
ENTRYPOINT ["/usr/bin/mithril"]
CMD ["/etc/mithril/mithril.conf.sample"]
