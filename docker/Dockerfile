FROM ubuntu:bionic-20210827
RUN apt update
RUN apt install -y build-essential g++-multilib make cpanminus git vim cmake wget

RUN wget https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3-linux-x86_64.tar.gz
RUN tar zxf cmake-3.21.3-linux-x86_64.tar.gz 
RUN rm cmake-3.21.3-linux-x86_64.tar.gz
RUN mv cmake-3.21.3-linux-x86_64/bin/* /usr/local/bin/
RUN mv cmake-3.21.3-linux-x86_64/share/* /usr/local/share/
RUN rm -rf cmake-3.21.3-linux-x86_64

RUN git config --global user.name "박철완"
RUN git config --global user.email "2020011630@hanyang.ac.kr"