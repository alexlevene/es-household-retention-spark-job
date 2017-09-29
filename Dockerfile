FROM 085170591206.dkr.ecr.us-east-1.amazonaws.com/spark-run:latest

ARG LOCAL_PATH
ENV LOCAL_PATH=${LOCAL_PATH}

WORKDIR /code

COPY ./es_ca.cer /usr/lib/jvm/java-1.8-openjdk/jre/lib/security/es_ca.cer
RUN keytool -import -alias elasticsearch -keystore /usr/lib/jvm/java-1.8-openjdk/jre/lib/security/cacerts -file /usr/lib/jvm/java-1.8-openjdk/jre/lib/security/es_ca.cer -storepass changeit -noprompt

# Add in important dependency related files to allow for caching
ADD Makefile /code/
ADD *.jar /code/

ENTRYPOINT ["make", "run-local"]
