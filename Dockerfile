# 该镜像主要用于非算法进程但是需要使用到scdap框架的项目
# 如scdap-apiserver/scdap-sqlapi等
# 另外也适用于gitlab流水线需要调用框架接口的情况
FROM ${SCDAP_IMAGE}

ADD ./scdap /pylib/scdap

# RUN sed -i "s@http://\(deb\|security\).debian.org@https://mirrors.aliyun.com@g" /etc/apt/sources.list \
#    && apt-get update && apt install ca-certificates && update-ca-certificates && apt-get -y install libsndfile1

ENV PYTHONPATH=/pylib \
    TZ=Asia/Shanghai \
    SCDAP_CI_COMMIT_REF_NAME=${CI_COMMIT_REF_NAME} \
    SCDAP_CI_COMMIT_SHORT_SHA=${CI_COMMIT_SHORT_SHA} \
    SCDAP_CI_COMMIT_SHA=${CI_COMMIT_SHA} \
    SCDAP_VERSION=${AUTO_GEN_VERSION}
