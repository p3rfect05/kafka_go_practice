FROM alpine:latest
RUN mkdir /app

WORKDIR /app
COPY ./myapp .



# Run the server executable
CMD [ "/app/myapp" ]