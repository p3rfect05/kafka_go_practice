FROM alpine:latest
RUN mkdir /app

COPY ./myapp /app


# Run the server executable
CMD [ "/app/myapp" ]