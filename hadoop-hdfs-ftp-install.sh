#!/bin/bash

cp -f hadoop-hdfs-ftp hadoop-hdfs-ftp-daemon /etc/init.d/
chmod +x /etc/init.d/hadoop-hdfs-ftp /etc/init.d/hadoop-hdfs-ftp-daemon
chkconfig --add hadoop-hdfs-ftp