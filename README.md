# Spark Project Setup

### Step 1: Choose a name for your job.
Choose an all-lower-case, hyphen-delimited name for job like
mdm-reconciliation-spark-job, campaign-contribution-spark-job.  
It should probably end with "spark-job"

### Step 2: Create an ECR Repository to hold docker images
Go to ECS (Elastic Container Service) in the AWS console
and select Repositories.  Create a new repository with the name
you chose above.

### Step 3: Clone and this repository
git clone

### Step 4: Rename the cloned directory and re-create it's git history
mv spark-job-template your-repository-name
cd your-repository-name

### Step 5: Modify build.sbt with the project name.
Change the projectName on the 1st line of build.sbt from SPARK_JOB_TEMPLATE
to exactly the name you used in steps 1 and 2.

### Step 6: Verification
sbt compile
sbt test
sbt run (should fail)
make build-docker
make run-local

### Step 7: Create a new repository in gitlab

### Step 8: Point this folder at the new gitlab repo
edit .git/config