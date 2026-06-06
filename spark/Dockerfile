# Spark image with the Iceberg + S3A jars baked in.
#
# Why bake the jars into the image (instead of --packages on spark-submit)?
#   * Reproducible, offline-at-runtime build: the cluster comes up without
#     depending on downloading dependencies from Maven on every submit.
#   * Pinned versions, avoiding "works on my machine".
#
# Base: Bitnami Spark 3.5.1 (Scala 2.12). Spark lives at /opt/bitnami/spark and
# the jars go to /opt/bitnami/spark/jars — the same directory spark-submit
# already loads onto the classpath.
FROM bitnamilegacy/spark:3.5.1

# We need root only to write into /opt/bitnami/spark/jars.
USER root

# Bitnami's minimal base does not ship curl — install it before the downloads.
RUN install_packages curl ca-certificates

# Pinned versions of the connectors.
#   iceberg-spark-runtime : Iceberg runtime for Spark 3.5 / Scala 2.12
#   iceberg-aws-bundle    : Iceberg's S3FileIO + AWS deps (s3:// scheme)
#   hadoop-aws            : Hadoop's S3AFileSystem (s3a:// scheme, reads raw Bronze)
#   aws-java-sdk-bundle   : AWS SDK required by hadoop-aws 3.3.x
ENV ICEBERG_VERSION=1.6.1 \
    HADOOP_AWS_VERSION=3.3.4 \
    AWS_SDK_VERSION=1.12.262

ARG MAVEN=https://repo1.maven.org/maven2
ARG JARS=/opt/bitnami/spark/jars

# curl already exists in the base; download the 4 jars and verify they are non-empty.
RUN set -eux; \
    curl -fSL "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        -o "${JARS}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fSL "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        -o "${JARS}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fSL "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        -o "${JARS}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fSL "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        -o "${JARS}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    for f in \
        "iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" ; do \
        test -s "${JARS}/${f}" || { echo "empty jar: ${f}" >&2; exit 1; }; \
    done

# Back to the Bitnami image's default non-root user.
USER 1001
