FROM golang:1.14
WORKDIR /go/src/app
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

CMD ["benchmark", "-file","/go/src/db/query_params.csv","-host","timescaledb"]