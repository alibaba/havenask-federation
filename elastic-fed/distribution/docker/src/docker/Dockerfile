################################################################################
# This Dockerfile was generated from the template at distribution/src/docker/Dockerfile
#
# Beginning of multi stage Dockerfile
################################################################################
<% /*
  This file is passed through Groovy's SimpleTemplateEngine, so dollars and backslashes
  have to be escaped in order for them to appear in the final Dockerfile. You
  can also comment out blocks, like this one. See:

  https://docs.groovy-lang.org/latest/html/api/groovy/text/SimpleTemplateEngine.html

  We use control-flow tags in this file to conditionally render the content. The
  layout/presentation here has been adjusted so that it looks reasonable when rendered,
  at the slight expense of how it looks here.
*/ %>
################################################################################
# Build stage 0 `builder`:
# Extract Havenask artifact
################################################################################

FROM ${base_image} AS builder

RUN mkdir /usr/share/havenask
WORKDIR /usr/share/havenask

${source_havenask}

RUN tar zxf /opt/havenask.tar.gz --strip-components=1
RUN sed -i -e 's/HAVENASK_DISTRIBUTION_TYPE=tar/HAVENASK_DISTRIBUTION_TYPE=docker/' /usr/share/havenask/bin/havenask-env
RUN mkdir -p config config/jvm.options.d data logs
RUN chmod 0775 config config/jvm.options.d data logs
COPY config/havenask.yml config/log4j2.properties config/
RUN chmod 0660 config/havenask.yml config/log4j2.properties

# add opensearch-dashboard
ADD "./packages/dashboards.tar.gz" "/usr/share/havenask/"

################################################################################
# Build stage 1 (the actual Havenask image):
#
# Copy havenask from stage 0
# Add entrypoint
################################################################################

FROM ${base_image}

ENV HAVENASK_CONTAINER true

RUN groupadd -g 1000 havenask && \\
    adduser -u 1000 -g 1000 -G 0 -d /usr/share/havenask havenask && \\
    chmod 0775 /usr/share/havenask && \\
    chown -R 1000:0 /usr/share/havenask

WORKDIR /usr/share/havenask
COPY --from=builder --chown=1000:0 /usr/share/havenask /usr/share/havenask

# Replace OpenJDK's built-in CA certificate keystore with the one from the OS
# vendor. The latter is superior in several ways.
# REF: https://github.com/elastic/elasticsearch-docker/issues/171
RUN ln -sf /etc/pki/ca-trust/extracted/java/cacerts /usr/share/havenask/jdk/lib/security/cacerts

ENV PATH /usr/share/havenask/bin:\$PATH

COPY bin/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# The JDK's directories' permissions don't allow `java` to be executed under a different
# group to the default. Fix this.
RUN find /usr/share/havenask/jdk -type d -exec chmod 0755 '{}' \\; && \\
    chmod g=u /etc/passwd && \\
    chmod 0775 /usr/local/bin/docker-entrypoint.sh

# Ensure that there are no files with setuid or setgid, in order to mitigate "stackclash" attacks.
RUN find / -xdev -perm -4000 -exec chmod ug-s {} +

EXPOSE 9200 9300

LABEL org.label-schema.build-date="${build_date}" \\
  org.label-schema.license="${license}" \\
  org.label-schema.name="Havenask" \\
  org.label-schema.schema-version="1.0" \\
  org.label-schema.url="https://www.havenask.org" \\
  org.label-schema.usage="https://www.havenask.org/guide/en/havenask/reference/index.html" \\
  org.label-schema.vcs-ref="${git_revision}" \\
  org.label-schema.vcs-url="https://github.com/alibaba/havenask-federation" \\
  org.label-schema.vendor="Havenask" \\
  org.label-schema.version="${version}" \\
  org.opencontainers.image.created="${build_date}" \\
  org.opencontainers.image.documentation="https://www.havenask.org/guide/en/havenask/reference/index.html" \\
  org.opencontainers.image.licenses="${license}" \\
  org.opencontainers.image.revision="${git_revision}" \\
  org.opencontainers.image.source="https://github.com/alibaba/havenask-federation" \\
  org.opencontainers.image.title="Havenask" \\
  org.opencontainers.image.url="https://www.havenask.org" \\
  org.opencontainers.image.vendor="Havenask" \\
  org.opencontainers.image.version="${version}"

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["havenaskwrapper"]

################################################################################
# End of multi-stage Dockerfile
################################################################################
