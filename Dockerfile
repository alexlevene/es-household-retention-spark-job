FROM 085170591206.dkr.ecr.us-east-1.amazonaws.com/spark-run:latest

ARG LOCAL_PATH
ENV LOCAL_PATH=${LOCAL_PATH}

WORKDIR /code

# Add in important dependency related files to allow for caching
ADD Makefile /code/
ADD *.jar /code/

ENTRYPOINT ["make", "run-local"]