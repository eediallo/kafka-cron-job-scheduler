# Build stage
FROM maven:3.9-eclipse-temurin-17-alpine AS builder

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn package -DskipTests

# Production runtime stage
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

COPY --from=builder /app/target/*-jar-with-dependencies.jar ./kafka-cron-job-scheduler.jar

COPY crontab.txt .

ENTRYPOINT ["java", "-jar", "kafka-cron-job-scheduler.jar"]
CMD ["crontab.txt"]