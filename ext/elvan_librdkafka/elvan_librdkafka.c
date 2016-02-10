#include "elvan_librdkafka.h"

static VALUE elvan_module;
#define ELVAN_MAX_ERRSTR_LEN 512

// Should we expect rb_thread_blocking_region to be present?
// #define RB_THREAD_BLOCKING_REGION
#undef RB_THREAD_BLOCKING_REGION

typedef struct {
    char *topic;
    /* Kafka configuration */
    rd_kafka_t* rd_kafka_inst;
    rd_kafka_conf_t* rd_kafka_conf;
    rd_kafka_topic_partition_list_t *topicPartitionList ;

    char errstr[512];
    char *group_id;
    char *client_id;
    char *brokers;
    char **initialTopics;

    int running;
    int exit_eof;
    int wait_eof;
    int silent;
    int isInitialized;
    int isConnected;
    int subscribed;

    char *error;
} Elvan_Config_t;

static void logger(const rd_kafka_t *rk,
                   int level,
                   const char *fac,
                   const char *buf) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
            (int)tv.tv_sec, (int)(tv.tv_usec / 1000),
            level, fac, rd_kafka_name(rk), buf);
}
char** str_split(char* a_str, const char a_delim)
{
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }

    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
     knows where the list of returned strings ends. */
    count++;

    result = malloc(sizeof(char*) * count);

    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            assert(idx < count);
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }

    return result;
}

void kafka_consumer_init(Elvan_Config_t* conf){

    conf->rd_kafka_conf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(conf->rd_kafka_conf, (void*)conf);

    if (!(conf->rd_kafka_inst = rd_kafka_new(RD_KAFKA_CONSUMER, conf->rd_kafka_conf,
                                             conf->errstr, sizeof(conf->errstr)))) {
        fprintf(stderr, "%% Failed to create new kafka consumer: %s\n", conf->errstr);
        rb_raise(rb_eRuntimeError, "%% Failed to create new kafka consumer: %s\n", conf->errstr);
    }



    /* Set logger */
    rd_kafka_conf_set_log_cb(conf->rd_kafka_conf, logger);
    rd_kafka_set_log_level(conf->rd_kafka_inst, LOG_DEBUG);

    if (conf->topicPartitionList == NULL){
        rb_raise(rb_eRuntimeError, "TopicPartitionList can't be empty\n");
    }

    if (rd_kafka_conf_set(conf->rd_kafka_conf, "group.id", conf->group_id,
                          conf->errstr, sizeof(conf->errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", conf->errstr);
        rb_raise(rb_eRuntimeError, "%% %s\n", conf->errstr);

    }

    if (rd_kafka_conf_set(conf->rd_kafka_conf, "bootstrap.servers", conf->brokers,
                          conf->errstr, sizeof(conf->errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", conf->errstr);
        rb_raise(rb_eRuntimeError, "%% %s\n", conf->errstr);

    }

    conf->isInitialized = 1;
}
static void print_partition_list (FILE *fp, int is_assigned,
                                  const rd_kafka_topic_partition_list_t
                                  *partitions, Elvan_Config_t* conf) {
    int i;
    for (i = 0 ; i < partitions->cnt ; i++) {
        fprintf(stderr, "%s %s [%"PRId32"]",
                i > 0 ? ",":"",
                partitions->elems[i].topic,
                partitions->elems[i].partition);

        if (is_assigned)
            conf->wait_eof++ ;
        else {
            if (conf->exit_eof && --conf->wait_eof == 0)
                conf->running = 0;
        }
    }
    fprintf(stderr, "\n");

}

static void rebalance_cb (rd_kafka_t *rk,
                          rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *partitions,
                          void *opaque) {

    Elvan_Config_t* conf = (Elvan_Config_t *)rd_kafka_opaque(rk);

    fprintf(stderr, "%% Consumer group rebalanced: ");
    switch (err)
    {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            fprintf(stderr, "assigned:\n");
            print_partition_list(stderr, 1, partitions, conf);
            rd_kafka_assign(rk, partitions);
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            fprintf(stderr, "revoked:\n");
            print_partition_list(stderr, 0, partitions, conf);
            rd_kafka_assign(rk, NULL);
            break;

        default:
            fprintf(stderr, "failed: %s\n",
                    rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
    }
}


static void elvan_consumer_free(void *p) {

    Elvan_Config_t *conf = (Elvan_Config_t *)p;

    if (conf->topicPartitionList != NULL) {
        rd_kafka_topic_partition_list_destroy(conf->topicPartitionList);
    }

    if (conf->rd_kafka_inst != NULL) {
        rd_kafka_destroy(conf->rd_kafka_inst);
    }

    free(conf->group_id);
    free(conf->brokers);
    free(conf);
}


static VALUE elvan_consumer_allocate(VALUE klass) {

    VALUE obj;
    Elvan_Config_t *conf;

    conf                     = ALLOC(Elvan_Config_t);
    conf->rd_kafka_inst      = NULL;
    conf->rd_kafka_conf      = NULL;
    conf->topicPartitionList = NULL;
    conf->group_id           = NULL;
    conf->brokers            = NULL;
    conf->initialTopics      = NULL;
    conf->silent             = 1;
    conf->errstr[0]          = 0;
    conf->running            = 0;
    conf->exit_eof           = 0;
    conf->wait_eof           = 0;
    conf->isInitialized      = 0;
    conf->subscribed         = 0;

    obj = Data_Wrap_Struct(klass, 0, elvan_consumer_free, conf);

    return obj;
}

static VALUE elvan_initialize(VALUE self,
                              VALUE brokers,
                              VALUE initialTopic,
                              VALUE group_id,
                              VALUE is_silent){

    Elvan_Config_t *conf;
    char* brokersPtr;
    char* group_idPtr;
    char* initialTopicPtr;
    int is_silentInt;

    brokersPtr   = StringValuePtr(brokers);
    group_idPtr  = StringValuePtr(group_id);
    initialTopicPtr = StringValuePtr(initialTopic);
    is_silentInt = FIX2INT(is_silent);

    Data_Get_Struct(self, Elvan_Config_t, conf);

    conf->brokers  = strdup(brokersPtr);
    conf->group_id = strdup(group_idPtr);
    conf->silent   = is_silentInt;

    conf->initialTopics = str_split(initialTopicPtr, ',');

    return self;
}

static void msg_consume(rd_kafka_message_t *rkmessage, Elvan_Config_t *conf){
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            if (conf->exit_eof) {
                fprintf(stderr,
                        "%% Consumer reached end of %s [%"PRId32"] "
                        "message queue at offset %"PRId64"\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        rkmessage->partition, rkmessage->offset);

                conf->running = 0;
            }

            return;
        }

        fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
                "offset %"PRId64": %s\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset,
                rd_kafka_message_errstr(rkmessage));
        return;
    }

//    if (DEBUG && rkmessage->key_len) {
//        if (output == OUTPUT_HEXDUMP) {
//            hexdump(stdout, "Message Key",
//                    rkmessage->key, rkmessage->key_len);
//        }
//        else {
//            printf("Key: %.*s\n",
//                   (int)rkmessage->key_len, (char *)rkmessage->key);
//        }
//    }

//    if (output == OUTPUT_HEXDUMP) {
//        if (DEBUG) {
//            hexdump(stdout, "Message Payload", rkmessage->payload, rkmessage->len);
//        }
//    }
//    else {
//        if (DEBUG) {
//            printf("%.*s\n", (int)rkmessage->len, (char *)rkmessage->payload);
//        }
//    }

    // Yield the data to the Consumer's block
    if (rb_block_given_p()) {
        VALUE key, data, offset;

        data = rb_str_new((char *)rkmessage->payload, rkmessage->len);
        offset = rb_ll2inum(rkmessage->offset);

        if ( rkmessage->key_len > 0 ) {
            key = rb_str_new((char*) rkmessage->key, (int)rkmessage->key_len);
        } else {
            key = Qnil;
        }

        rd_kafka_message_destroy(rkmessage);
        rb_yield_values(3, data, key, offset);
    }
    else {
      fprintf(stderr, "No block given\n"); // todo: should this be an error?
    }

}

static VALUE elvan_consumer_loop(VALUE self){
    Elvan_Config_t* conf;
    Data_Get_Struct(self, Elvan_Config_t, conf);

    while (conf->running) {
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(conf->rd_kafka_inst, 500);
        if (rkmessage) {
            msg_consume(rkmessage, conf);
        }
    }
    return self;
}


static VALUE elvan_consumer_loop_stop(VALUE self, int attempt){
    Elvan_Config_t* conf;
    rd_kafka_resp_err_t rd_err;

    Data_Get_Struct(self, Elvan_Config_t, conf);

    rd_err = rd_kafka_consumer_close(conf->rd_kafka_inst);
    if (rd_err)
        fprintf(stderr, "%% Failed to close consumer: %s\n", rd_kafka_err2str(rd_err));
    else
        fprintf(stderr, "%% Consumer closed\n");

    if(5>=attempt){
        elvan_consumer_loop_stop(self, attempt+1);
    }
    return Qnil;
}

static VALUE elvan_consume(VALUE self){
    Elvan_Config_t* conf;
    rd_kafka_resp_err_t rd_err;

    Data_Get_Struct(self, Elvan_Config_t, conf);

    if(!conf->isInitialized){
        kafka_consumer_init(conf);
    }

    if(!conf->subscribed){
        if ((rd_err = rd_kafka_subscribe(conf->rd_kafka_inst, conf->topicPartitionList))) {
            fprintf(stderr,
                    "%% Failed to start consuming topics: %s\n",
                    rd_kafka_err2str(rd_err));
            rb_raise(rb_eRuntimeError, "Failed to start consuming topic");
            return Qnil;
        }
    }else{
        if((rd_err = rd_kafka_assign(conf->rd_kafka_inst, conf->topicPartitionList))){
            fprintf(stderr,
                    "%% Failed to assign topics partition: %s\n",
                    rd_kafka_err2str(rd_err));
            rb_raise(rb_eRuntimeError, "Failed to assign topics partition");
            return Qnil;

        };
    }


    return rb_ensure(elvan_consumer_loop, self, elvan_consumer_loop_stop, self);
}

void Init_elvan_librdkafka() {
    VALUE consumer_klass, rd_kafka_module;
    elvan_module = rb_define_module("Elvan");
    rd_kafka_module = rb_define_module_under(elvan_module, "RdKafka");
    consumer_klass = rb_define_class_under(rd_kafka_module, "Consumer", rb_cObject);

    rb_define_alloc_func(consumer_klass, elvan_consumer_allocate);
    rb_define_method(consumer_klass, "initialize", elvan_initialize, 4);
    rb_define_method(consumer_klass, "on_message", elvan_consume, 0);
}