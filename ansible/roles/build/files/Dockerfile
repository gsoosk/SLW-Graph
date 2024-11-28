FROM openjdk:23-jdk-bullseye
RUN apt-get update && \
    apt-get install iproute2 iputils-ping -y
WORKDIR /app
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
CMD ["--help"]