#! /bin/bash

echo installing maven ...
wget ftp://ftp.fau.de/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
su -c "tar -zxvf apache-maven-3.0.5-bin.tar.gz -C /opt/" 
rm -f apache-maven-3.0.5-bin.tar.gz
export M2_HOME=/opt/apache-maven-3.0.5
export M2=$M2_HOME/bin
export PATH=$M2:$PATH
echo "export M2_HOME=$M2_HOME" >> ~/.bashrc
echo "export M2=\$M2_HOME/bin" >> ~/.bashrc
echo "export PATH=\$M2:\$PATH" >> ~/.bashrc
echo -e "if [ -f \"\$HOME/.bashrc\" ]; then\n  source \"\$HOME/.bashrc\"\nfi" >> ~/.bash_profile
echo done!

