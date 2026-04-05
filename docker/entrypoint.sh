#!/bin/bash

# 1. Khởi động dịch vụ SSH (quyền root)
/usr/sbin/sshd

# 2. Đòi lại quyền sở hữu thư mục SSH cho user hadoop (VĨNH VIỄN)
chown -R hadoop:hadoop /home/hadoop/.ssh
chmod 700 /home/hadoop/.ssh
chmod 600 /home/hadoop/.ssh/id_rsa
chmod 644 /home/hadoop/.ssh/id_rsa.pub
chmod 600 /home/hadoop/.ssh/authorized_keys

# 3. SỬA LỖI QUYỀN VOLUME (BẮT BUỘC để không bị Access Denied)
# Lệnh này giúp user hadoop luôn có quyền ghi vào folder mount từ Windows
chown -R hadoop:hadoop /opt/hadoop/data
chmod -R 755 /opt/hadoop/data

# 4. Duy trì container chạy ngầm
tail -f /dev/null