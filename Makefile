BASE_JAR_NAME=es-household-retention-spark-job
GIT_REV=$(shell git rev-parse --short HEAD)
APP_VERSION?=$(GIT_REV)
LOCAL_PATH?="$(BASE_JAR_NAME)-assembly-$(GIT_REV).jar"
LOCAL_FULL_PATH=./target/scala-2.11/$(BASE_JAR_NAME)-assembly-$(GIT_REV).jar
S3_BASE_BUCKET=exp-spark-jars
S3_PATH="s3://$(S3_BASE_BUCKET)/$(BASE_JAR_NAME)/$(LOCAL_PATH)"
LOCAL_BUILD_PREFIX=local-build-
LOCAL_PREFIX=local-
MAIN_CLASS="com.healthgrades.edp.spark.HouseholdRetentionProcessing"
ECR_BASE_NAME="085170591206.dkr.ecr.us-east-1.amazonaws.com"
ECR_IMAGE=$(BASE_JAR_NAME)
STEP_NAME=HouseholdRetentionProcessing
EMRFS_STEP_NAME=$(subst SparkJob,EMRFSCleanup,$(STEP_NAME))
# cluster one - not preferrred - (running zeppelin) - dev-exp-emr (https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-1FYW32NU8NWIO)
# EMR_CLUSTER="j-1FYW32NU8NWIO"
# cluster two - preferrred - dev-exp-emr-2 (https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:j-3FB4H204JBWEN)
EMR_CLUSTER="j-3FB4H204JBWEN"
REMOTE_PATH=/home/hadoop/$(LOCAL_PATH)
REMOTE_HOST=hadoop@ec2-54-236-14-0.compute-1.amazonaws.com

REMOTE_ARGS=$(shell echo $(ARGS) | tr " " ",")
EMR_KEY_PATH=~/.ssh/dev-exp-client-data.pem

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
	docker run -it --rm -e ES_HOST="100.70.102.71" -e CLIENT_CODE="DEMO" -e ES_INDEX="exp_rjj_1_2" -e SPARK_PROCESS_BATCH_SIZE="10000" ${LOCAL_PREFIX}${ECR_IMAGE}

# NOTE: This can only be used locally right now due to Docker version concerns
# Multi-stage builds are only available in Docker CE 17+ and currently our Kubernetes
# and jenkins environments aren't there yet
build-multistage:
	docker build --file Dockerfile.multistage --build-arg="APP_VERSION=$(GIT_REV)" -t multistage-${ECR_IMAGE} .

push-docker:
	eval $(aws ecr get-login --region us-east-1)
	docker push ${LOCAL_PREFIX}${ECR_IMAGE}:$(GIT_REV)

update:
	sbt update

assembly:
	sbt 'set version := "$(APP_VERSION)"' assembly

test:
	sbt test

# this is running in client mode with a variable number of cores (the *)
run-local:
ifeq ($(strip $(ARGS)),)
	/opt/spark/bin/spark-submit --master local[*] $(LOCAL_PATH)
else
	/opt/spark/bin/spark-submit --master local[*] $(LOCAL_PATH) $(ARGS)
endif

s3-upload: build-docker
	aws s3 cp $(LOCAL_PATH) $(S3_PATH)

# sudo sysctl -w net.inet.tcp.sack=0
# sudo ifconfig en4 mtu 576
upload: assembly
	scp -C -i $(EMR_KEY_PATH) $(LOCAL_FULL_PATH) $(REMOTE_HOST):$(REMOTE_PATH)
#	scp -v  -C -l 8192 -i $(EMR_KEY_PATH) $(LOCAL_FULL_PATH) $(REMOTE_HOST):$(REMOTE_PATH)

run-remote: s3-upload
ifeq ($(strip $(ARGS)),)
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(BASE_JAR_NAME)",ActionOnFailure=CONTINUE,Args=[--class,$(MAIN_CLASS),$(S3_PATH)]
else
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(BASE_JAR_NAME)",ActionOnFailure=CONTINUE,Args=[--class,$(MAIN_CLASS),$(S3_PATH),$(REMOTE_ARGS)]
endif



run-remote-cluster-mode: s3-upload
ifeq ($(strip $(ARGS)),)
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(BASE_JAR_NAME)",ActionOnFailure=CONTINUE,Args=[--master,yarn,--deploy-mode,cluster,--class,$(MAIN_CLASS),$(S3_PATH)]
else
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(BASE_JAR_NAME)",ActionOnFailure=CONTINUE,Args=[--master,yarn,--deploy-mode,cluster,--class,$(MAIN_CLASS),$(S3_PATH),$(REMOTE_ARGS)]
endif

# make run-remote-client-mode ARGS="DEMO elasticsearch.exp-dev.io exp_rjj_1_2 10000"
# make run-remote-client-mode  ARGS="DEMO 100.70.102.71 exp_rjj_1_2 10000"
# run-remote-client-mode: upload
run-remote-client-mode:
ifeq ($(strip $(ARGS)),)
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(STEP_NAME)",ActionOnFailure=CONTINUE,Args=[--class,$(MAIN_CLASS),$(REMOTE_PATH)]
else
	aws emr add-steps --cluster-id $(EMR_CLUSTER) --steps Type=Spark,Name="$(STEP_NAME)",ActionOnFailure=CONTINUE,Args=[--class,$(MAIN_CLASS),$(REMOTE_PATH),$(REMOTE_ARGS)]
endif
