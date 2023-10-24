usage() {
  echo -e "Usage: $0 [-t <topic>] [-i <kafka install path>] [-p <partitions>] [-r <replications>] [-z <zookeeper>]\n"\
       "where\n"\
       "-i defines kafka install directory\n"\
       "-p defines number of partitions\n"\
       "-r defines replication factor\n"\
       "-t defines topic name\n"\
       "-z defines zookeeper url\n"\
       "\n"\
        1>&2
  exit 1
}

while getopts ":i:p:r:t:z:" opt; do
    case "$opt" in
        i)  KAFKA_INSTALL_PATH=${OPTARG} ;;
        p)  PARTITIONS=${OPTARG} ;;
        r)  REPLICATION_FACTOR=${OPTARG} ;;
        t)  TOPIC_NAME=${OPTARG} ;;
        t)  ZOOKEEPER_URL=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$KAFKA_INSTALL_PATH" ]];
then
  KAFKA_INSTALL_PATH="/usr/lib/kafka/bin"
fi

if [[ -z "$PARTITIONS" ]];
then
  PARTITIONS="3"
fi

if [[ -z "$REPLICATION_FACTOR" ]];
then
  REPLICATION_FACTOR="1"
fi

if [[ -z "$ZOOKEEPER_URL" ]];
then
  ZOOKEEPER_URL="localhost:2181"
fi

[[ -z "$TOPIC_NAME" ]] && { echo "TOPIC_NAME is empty" ; exit 1; }

KAFKA_TOPICS_FILE="kafka-topics.sh"
KAFKA_TOPICS_COMMAND="$KAFKA_INSTALL_PATH/$KAFKA_TOPICS_FILE"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo "KAFKA_INSTALL_PATH = $KAFKA_INSTALL_PATH"
echo "PARTITIONS = $PARTITIONS"
echo "REPLICATION_FACTOR = $REPLICATION_FACTOR"
echo "TOPIC_NAME = $TOPIC_NAME"
echo "-------------------------------------"
echo "KAFKA_TOPICS_COMMAND = $KAFKA_TOPICS_COMMAND"

#kafka-topics.sh --zookeeper localhost:2181 --topic first_topic --create --partitions 3 --replication-factor 1

CREATE_TOPIC_CMD="${KAFKA_TOPICS_COMMAND} --zookeeper ${ZOOKEEPER_URL} --topic ${TOPIC_NAME} --create --partitions ${PARTITIONS} --replication-factor ${REPLICATION_FACTOR}"
echo "CREATE_TOPIC_CMD"
${CREATE_TOPIC_CMD}

echo "<<<<<<<<<<<<<<<<<<  Topic ${TOPIC_NAME} has been created  <<<<<<<<<<<<<<<<<<<<<"

echo "describing topic "
#kafka-topics.sh --zookeeper localhost:2181 --describe --topic first_topic
DESCRIBE_TOPIC_CMD="${KAFKA_TOPICS_COMMAND} --zookeeper ${ZOOKEEPER_URL} --describe --topic ${TOPIC_NAME}"
echo "DESCRIBE_TOPIC_CMD"
${DESCRIBE_TOPIC_CMD}
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
