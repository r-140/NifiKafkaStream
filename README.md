if unit tests failed with timeout error add
PYSPARK_PYTHON=python
environment variable

to run from gcp console run
connect to dataproc cluster
gcloud compute ssh procamp-cluster-m --zone=us-east1-b --project=<project_id>
clone this project and go to cloned directory

run next:
gcloud dataproc jobs submit pyspark lab8.py --cluster=procamp-cluster --region=us-east1 --py-files lab8util.py --properties spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 -- --bucket <gcp_bucket> --folder <output_folder> --topic <topic_name>


For unit test MEmory stream is being used see Lab8It.py for details



