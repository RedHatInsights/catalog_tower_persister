FROM registry.redhat.io/ubi8/go-toolset:1.15.14

WORKDIR /go/src/app
COPY . .

USER 0

RUN go get -d ./... && \
    go install -v ./...

RUN cp /opt/app-root/src/go/bin/catalog_tower_persister /usr/bin/

RUN yum remove -y kernel-headers npm nodejs nodejs-full-i18n && yum update -y && yum clean all

USER 1001
CMD ["catalog_tower_persister"]
