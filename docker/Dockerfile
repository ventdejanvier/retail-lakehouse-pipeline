# Sử dụng Ubuntu bản mới nhất hỗ trợ JDK 21 cực tốt
FROM ubuntu:24.04

USER root

# 1. Cài đặt các gói cơ bản và JDK 21
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk ssh pdsh wget vim iputils-ping sudo curl \
    && apt-get clean

# 2. Thiết lập Biến môi trường hệ thống (Cực kỳ quan trọng)
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV SPARK_CONF_DIR=$SPARK_HOME/conf

# Gom tất cả đường dẫn thực thi vào PATH một lần duy nhất
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 3. Cài đặt Hadoop 3.4.3
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz && \
    tar -xzvf hadoop-3.4.3.tar.gz && \
    mv hadoop-3.4.3 $HADOOP_HOME && \
    rm hadoop-3.4.3.tar.gz

# 4. Cài đặt Spark 3.5.1 (Bản chuẩn chạy trên Java 21)
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 $SPARK_HOME && \
    rm spark-3.5.1-bin-hadoop3.tgz

# 5. Tạo User hadoop và các thư mục hệ thống
RUN useradd -m -s /bin/bash hadoop && \
    echo "hadoop:hadoop" | chpasswd && \
    adduser hadoop sudo && \
    mkdir -p /var/run/sshd /opt/hadoop/data/nameNode /opt/hadoop/data/dataNode

# 6. Phân quyền toàn bộ thư mục cho user hadoop
RUN chown -R hadoop:hadoop /opt/hadoop && \
    chown -R hadoop:hadoop /opt/spark && \
    chown root:root /var/run/sshd && \
    chmod 755 /var/run/sshd

# 7. Ép các dịch vụ Hadoop chạy dưới quyền user hadoop
ENV HDFS_NAMENODE_USER=hadoop
ENV HDFS_DATANODE_USER=hadoop
ENV HDFS_SECONDARYNAMENODE_USER=hadoop
ENV YARN_RESOURCEMANAGER_USER=hadoop
ENV YARN_NODEMANAGER_USER=hadoop

# 8. Cấu hình SSH nội bộ (Để các node tự nhận nhau không cần mật khẩu)
USER hadoop
WORKDIR /home/hadoop
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> ~/.ssh/config

# 9. Nạp file Entrypoint (Trở lại quyền root để nạp file hệ thống)
USER root
# Lưu ý: File entrypoint.sh phải nằm cùng thư mục với Dockerfile này
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# 10. Chạy bằng Entrypoint (Root lo hạ tầng, Hadoop lo dịch vụ)
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]