#! /bin/bash

wget ftp://ftp.fau.de/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
su -c "tar -zxvf apache-maven-3.0.5-bin.tar.gz -C /opt/" 
export M2_HOME=/opt/apache-maven-3.0.5
export M2=$M2_HOME/bin
export PATH=$M2:$PATH
