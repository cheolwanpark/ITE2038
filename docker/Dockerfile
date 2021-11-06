FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Seoul

RUN apt update
RUN apt-get install -y tzdata
RUN apt install -y build-essential g++-multilib make cpanminus git vim cmake wget gcc-10 g++-10
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 90
RUN update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 90

RUN wget https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3-linux-x86_64.tar.gz
RUN tar zxf cmake-3.21.3-linux-x86_64.tar.gz 
RUN rm cmake-3.21.3-linux-x86_64.tar.gz
RUN mv cmake-3.21.3-linux-x86_64/bin/* /usr/local/bin/
RUN mv cmake-3.21.3-linux-x86_64/share/* /usr/local/share/
RUN rm -rf cmake-3.21.3-linux-x86_64

RUN git config --global user.name "박철완"
RUN git config --global user.email "2020011630@hanyang.ac.kr"