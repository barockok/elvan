#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <ctype.h>
#include <errno.h>
#include <syslog.h>
#include <assert.h>
#include "ruby.h"
#include "librdkafka/rdkafka.h"

typedef struct {
    VALUE consumer_config_hash;
    VALUE initialTopics;
    char errstr[512];
    int exit_eof;
    int subscribed;
    int wait_eof;
    int running;
    rd_kafka_t *rd_kafka_inst;
    rd_kafka_conf_t* rd_kafka_conf;
    rd_kafka_topic_partition_list_t *topic_partitions;

} Elvan_Config_t;