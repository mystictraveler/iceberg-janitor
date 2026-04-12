FROM sbtscala/scala-sbt:eclipse-temurin-jammy-17.0.10_7_1.9.9_2.12.18 AS build
WORKDIR /app
COPY build.sbt .
COPY project/ project/
RUN sbt update
COPY src/ src/
RUN sbt assembly

FROM scratch AS export
COPY --from=build /app/target/scala-2.12/spark-compare-assembly.jar /spark-compare-assembly.jar
