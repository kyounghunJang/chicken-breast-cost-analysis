FROM --platform=linux/amd64 apache/airflow:2.3.3-python3.8
COPY lib.txt /lib.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /lib.txt
USER root
RUN sudo curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add
RUN sudo apt-get -y update
RUN sudo apt-get install wget

RUN sudo apt-get -y install openjdk-11-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb 
RUN sudo apt install -y ./google-chrome-stable_current_amd64.deb
RUN apt-get install -yqq unzip
RUN echo "Chrome: " && google-chrome --version

#install chromedriver
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/bin/chromedriver
ENV DISPLAY=:99

RUN sudo chmod 755 "/usr/bin/chromedriver"
RUN sudo chmod 755 "/usr/bin/google-chrome-stable"
USER airflow