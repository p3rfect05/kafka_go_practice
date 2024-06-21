FROM golang 

COPY . . 

RUN set GOOS=linux&& set GOARCH=amd64&& set CGO_ENABLED=0 && go build -o myapp ./

RUN go install github.com/IBM/sarama/tools/kafka-console-consumer
RUN ./myapp
RUN kafka-console-consumer -topic=topic-test -brokers=kafka:9092 -offset=newest