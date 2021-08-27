FROM golang:1.17

WORKDIR /go/src
ENV PATH="go/bin/${PATH}"

COPY . .

RUN apt-get update && \
    apt-get install build-essential librdkafka-dev -y

CMD ["tail", "-f", "dev/null"]
