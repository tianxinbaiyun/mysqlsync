FROM alpine:latest

ADD server /

ADD config.yaml /

CMD ["/server"]