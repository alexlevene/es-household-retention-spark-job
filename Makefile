BASE_JAR_NAME = "SPARK_JOB_TEMPLATE"
GIT_REV=$(shell git rev-parse --short HEAD)
LOCAL_PATH ?= "$(BASE_JAR_NAME)-assembly-$(GIT_REV).jar"
S3_BASE_BUCKET = "exp-spark-jars"
S3_PATH = "s3://$(S3_BASE_BUCKET)/$(BASE_JAR_NAME)/$(LOCAL_PATH)"
LOCAL_BUILD_PREFIX="local-build-"
LOCAL_PREFIX="local-"
MAIN_CLASS="Main"
ECR_BASE_NAME="085170591206.dkr.ecr.us-east-1.amazonaws.com"
ECR_IMAGE=$(BASE_JAR_NAME)
EMR_CLUSTER="j-1FYW32NU8NWIO"

# A bit meta...I know, but it is easier to pass in git info than rely on it in our image
build-docker:
	# Do the actual build
	docker build --file Dockerfile.build --build-arg="APP_VERSION=$(GIT_REV)" -t ${LOCAL_BUILD_PREFIX}${ECR_IMAGE}:latest -t ${LOCAL_BUILD_PREFIX}${ECR_IMAGE}:$(GIT_REV) .
	# Create an intermittent container that we can copy the JAR from
	docker create --name $(BASE_JAR_NAME)-$(GIT_REV) ${LOCAL_BUILD_PREFIX}${ECR_IMAGE}:$(GIT_REV)
	docker cp $(BASE_JAR_NAME)-$(GIT_REV):/code/target/scala-2.11/$(LOCAL_PATH) .
	docker rm -f $(BASE_JAR_NAME)-$(GIT_REV)

	# Pass in the actual local path at build time because the running container
	# doesn't have git available
	docker build --build-arg="LOCAL_PATH=$(LOCAL_PATH)" -t ${LOCAL_PREFIX}${ECR_IMAGE} -t ${ECR_BASE_NAME}/${ECR_IMAGE}:${GIT_REV} .

# Simple version of the command that assumes you have a connection to dev ES
int-test-local:
	docker run -it --rm -e ES_HOST="elasticsearch.exp-dev.io" ${LOCAL_PREFIX}${ECR_IMAGE}

# NOTE: This can only be used locally right now due to Docker version concerns
# Multi-stage builds are only available in Docker CE 17+ and currently our Kubernetes
# and jenkins environments aren't there yet
build-multistage:
	docker build --file Dockerfile.multistage --build-arg="APP_VERSION=$(GIT_REV)" -t multistage-${ECR_IMAGE} .

update:
	sbt update

assembly:
	sbt 'set version := "$(APP_VERSION)"' assembly

test:
	sbt test

run-local:
ifeq ($(strip $(ARGS)),)
	/opt/spark/bin/spark-submit --master local[*] $(LOCAL_PATH)
else
	/opt/spark/bin/spark-submit --master local[*] $(LOCAL_PATH) $(ARGS)
endif

s3-upload: build-docker
	aws s3 cp $(LOCAL_PATH) $(S3_PATH)

upload: assembly
	scp -v -C -l 8192 -i $(EMR_KEY_PATH) $(LOCAL_PATH) $(REMOTE_HOST):$(REMOTE_PATH)

run-remote: s3-upload
ifeq ($(strip $(ARGS)),)
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(BASE_JAR_NAME)",ActionOnFailure=CONTINUE,Args=[--class,$(MAIN_CLASS),$(S3_PATH)]
else
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(BASE_JAR_NAME)",ActionOnFailure=CONTINUE,Args=[--class,$(MAIN_CLASS),$(S3_PATH),$(REMOTE_ARGS)]
endif
