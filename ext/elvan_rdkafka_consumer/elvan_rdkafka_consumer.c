#include "elvan_rdkafka_consumer.h"

static VALUE elvan_module;

// Utility Methods
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
// Helper Methods
static void helper__wait_kafka_destroy(rd_kafka_t *rk,
                                       int wait_to,
                                       int max_attempt){

    while (max_attempt-- > 0 && rd_kafka_wait_destroyed(wait_to) == -1)
        fprintf(stderr, "Waiting for librdkafka to decommission\n");

    if (max_attempt <= 0)
        fprintf(stderr, "librdkafka to decommissioned\n");
}

static void helper__print_partition_list (FILE *fp, int is_assigned,
                                          const rd_kafka_topic_partition_list_t
                                          *partitions, Elvan_Config_t* conf) {
    int i;
    for (i = 0 ; i < partitions->cnt ; i++) {
        fprintf(stderr, "%s %s parition: [%"PRId32"] offset: [%"PRId32"]",
                i > 0 ? ",":"",
                partitions->elems[i].topic,
                partitions->elems[i].partition,
                partitions->elems[i].offset
                );

        if (is_assigned)
            conf->wait_eof++ ;
        else {
            if (conf->exit_eof && conf->wait_eof == 0)
                conf->running = 0;
        }
    }
    fprintf(stderr, "\n");
}

static void helper__retrive_initial_partitionList_position(Elvan_Config_t* conf){
    rd_kafka_resp_err_t err;

    err = rd_kafka_position(conf->rd_kafka_inst, conf->topic_partitions, 1000);

    if (err)
        rb_raise(rb_eRuntimeError, "%% Failed to fetch offsets: %s\n", rd_kafka_err2str(err));
}

static void helper__rebalance_cb (rd_kafka_t *rk,
                                  rd_kafka_resp_err_t err,
                                  rd_kafka_topic_partition_list_t *partitions,
                                  void *opaque) {

    Elvan_Config_t* conf = (Elvan_Config_t *)rd_kafka_opaque(rk);

    fprintf(stderr, "%% Consumer group rebalanced: ");
    switch (err)
    {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            fprintf(stderr, "assigned:\n");
            helper__print_partition_list(stderr, 1, partitions, conf);
            rd_kafka_assign(rk, partitions);
            break;

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            fprintf(stderr, "revoked:\n");
            helper__print_partition_list(stderr, 0, partitions, conf);
            rd_kafka_assign(rk, NULL);
            break;

        default:
            fprintf(stderr, "failed: %s\n",
                    rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
    }
}

static void *helper__consumer_recv_msg(void *ptr){
		Elvan_Config_t *conf = (Elvan_Config_t *) ptr;
    rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(conf->rd_kafka_inst, conf->message_poll_timeout);
    if ( rkmessage == NULL ) {
        if ( errno != ETIMEDOUT )
            fprintf(stderr, "%% Error: %s\n", rd_kafka_err2str( rd_kafka_errno2err(errno)));
    }
    return (void *) rkmessage;
}


static void helper__parse_initialTopic_to_partitionList(Elvan_Config_t* conf){
    conf->topic_partitions = rd_kafka_topic_partition_list_new(1);
    VALUE topicsCnt = rb_funcall(conf->initialTopics, rb_intern("size"), 0);
    int topicsCntInt = FIX2INT(topicsCnt);

    for (int i = 0;  i < topicsCntInt ; i++)
    {
        VALUE Vtopic_name = rb_ary_entry(conf->initialTopics, i);
        char *topic_name = StringValuePtr(Vtopic_name);
        rd_kafka_topic_partition_list_add(conf->topic_partitions, topic_name, -1);
    }
}

static void helper__init_kafka_conf(Elvan_Config_t *conf){
    conf->rd_kafka_conf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(conf->rd_kafka_conf, (void*)conf);

    VALUE options_keys = rb_funcall(conf->consumer_config_hash, rb_intern("keys"), 0);
    int options_keys_size = FIX2INT(rb_funcall(options_keys, rb_intern("size"), 0));

    for (int i = 0; i < options_keys_size; i++) {
        VALUE VKey, VVal;
        VKey = rb_ary_entry(options_keys, i);
        VVal = rb_hash_aref(conf->consumer_config_hash, VKey);
        char *key = StringValuePtr(VKey);
        char *val = StringValuePtr(VVal);
        if (rd_kafka_conf_set(conf->rd_kafka_conf, key, val,
                              conf->errstr, sizeof(conf->errstr)) != RD_KAFKA_CONF_OK) {
            rb_raise(rb_eRuntimeError, "%% %s\n", conf->errstr);

        }
    }

    /* Set logger */
    rd_kafka_conf_set_log_cb(conf->rd_kafka_conf, logger);
    rd_kafka_conf_set_rebalance_cb(conf->rd_kafka_conf, helper__rebalance_cb);

}

static void helper__init_kafka_consumer(Elvan_Config_t* conf){
    if (!(conf->rd_kafka_inst = rd_kafka_new(RD_KAFKA_CONSUMER, conf->rd_kafka_conf,
                                             conf->errstr, sizeof(conf->errstr)))) {
        rb_raise(rb_eRuntimeError, "%% Failed to create new kafka consumer: %s\n", conf->errstr);
    }

    rd_kafka_set_log_level(conf->rd_kafka_inst, LOG_DEBUG);

    helper__parse_initialTopic_to_partitionList(conf);
    helper__retrive_initial_partitionList_position(conf);

    if (conf->topic_partitions == NULL){
        rb_raise(rb_eRuntimeError, "TopicPartitionList can't be empty\n");
    }
}

static void helper__msg_consume(rd_kafka_message_t *rkmessage, Elvan_Config_t *conf){
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            if (conf->exit_eof) {
                fprintf(stderr,
                        "%% Consumer reached end of %s [%"PRId32"] "
                        "message queue at offset %"PRId64"\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        rkmessage->partition, rkmessage->offset);

                //                conf->running = 0;
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
        rb_raise(rb_eArgError, "no block given" );
    }

}

static VALUE helper__consumer_loop_stop(VALUE self){
    Elvan_Config_t* conf;
    rd_kafka_resp_err_t rd_err;

    Data_Get_Struct(self, Elvan_Config_t, conf);

    rd_err = rd_kafka_unsubscribe(conf->rd_kafka_inst);
    if (rd_err){
        rb_raise(rb_eRuntimeError, "%% Failed to close unsubscribe: %s\n", rd_kafka_err2str(rd_err));
        return Qnil;
    }else
        fprintf(stderr, "%% Success unsubscribe\n");

    rd_err = rd_kafka_consumer_close(conf->rd_kafka_inst);
    if(rd_err){
        rb_raise(rb_eRuntimeError, "%% Failed to close consumer: %s\n", rd_kafka_err2str(rd_err));
        return Qnil;
    }else
        fprintf(stderr, "%% Success closed\n");

    conf->running = 0;
    conf->subscribed = 0;

    return Qnil;
}

static void helper__elvan_consumer_stop_cb(void *ptr) {
    Elvan_Config_t* conf = (Elvan_Config_t*)ptr;
    conf->running = 0;
}

static VALUE helper__consumer_loop(VALUE self){

    Elvan_Config_t* conf;
    rd_kafka_message_t *rkmessage;
    Data_Get_Struct(self, Elvan_Config_t, conf);

    while (conf->running) {
#if HAVE_RB_THREAD_BLOCKING_REGION && RUBY_API_VERSION_MAJOR < 2
        rkmessage = (rd_kafka_message_t *) rb_thread_blocking_region((rb_blocking_function_t *) helper__consumer_recv_msg,
                                                                     conf,
                                                                     helper__elvan_consumer_stop_cb,
                                                                     conf);
#elif HAVE_RB_THREAD_CALL_WITHOUT_GVL
        rkmessage = rb_thread_call_without_gvl(helper__consumer_recv_msg,
                                               conf,
                                               helper__elvan_consumer_stop_cb,
                                               conf);
#else
        rkmessage = helper__consumer_recv_msg(conf);
#endif
        if (rkmessage)
            helper__msg_consume(rkmessage, conf);

    }
    return Qnil;
}

// Elvan::RdKafka::Consumer istance Methods
static void elvan_consumer_free(void *p) {

    Elvan_Config_t *conf = (Elvan_Config_t *)p;

    if (conf->topic_partitions != NULL) {
        rd_kafka_topic_partition_list_destroy(conf->topic_partitions);
    }

    if (conf->rd_kafka_inst != NULL) {
        rd_kafka_destroy(conf->rd_kafka_inst);
        helper__wait_kafka_destroy(conf->rd_kafka_inst, 1000, 5);
    }

    free(conf);
}
static VALUE elvan_consumer_allocate(VALUE klass) {

    VALUE obj;
    Elvan_Config_t *conf;

    conf                     = ALLOC(Elvan_Config_t);
    conf->errstr[0]          = 0;
    conf->exit_eof           = 0;
    conf->subscribed         = 0;

    obj = Data_Wrap_Struct(klass, 0, elvan_consumer_free, conf);
    return obj;
}
static VALUE elvan_initialize(VALUE self,
                              VALUE initialTopic,
                              VALUE consumer_conf
                              ){

    Elvan_Config_t *conf;
    int exit_eof, message_poll_timeout, max_wait_brokers_down;
    VALUE Vexit_eof, Vmax_wait_brokers_down, Vmessage_poll_timeout;

    Vexit_eof = rb_hash_delete(consumer_conf, rb_str_new2("exit_eof"));
    exit_eof = NIL_P(Vexit_eof) ? 0 : FIX2INT(Vexit_eof) ;

    Vmax_wait_brokers_down = rb_hash_delete(consumer_conf, rb_str_new2("max_wait_brokers_down"));
    max_wait_brokers_down = NIL_P(Vmax_wait_brokers_down) ? 5000 : FIX2INT(Vmax_wait_brokers_down);

    Vmessage_poll_timeout = rb_hash_delete(consumer_conf, rb_str_new2("message_poll_timeout"));
    message_poll_timeout = NIL_P(Vmessage_poll_timeout) ? 500 : FIX2INT(Vmessage_poll_timeout);

    Data_Get_Struct(self, Elvan_Config_t, conf);

    conf->initialTopics        = initialTopic;
    conf->consumer_config_hash = consumer_conf;
    conf->exit_eof             = exit_eof;
    conf->running              = 0;
    conf->wait_eof             = 0;
    conf->brokers_down         = 0;
    conf->max_wait_brokers_down= max_wait_brokers_down;
    conf->message_poll_timeout = message_poll_timeout;

    return self;
}

static VALUE elvan_consume(VALUE self){

    Elvan_Config_t* conf;
    rd_kafka_resp_err_t rd_err;

    Data_Get_Struct(self, Elvan_Config_t, conf);

    helper__init_kafka_conf(conf);
    helper__init_kafka_consumer(conf);
    VALUE VGroupId  = rb_hash_aref(conf->consumer_config_hash, rb_str_new2("group.id"));
    char *group_id = StringValuePtr(VGroupId);

    if ((rd_err = rd_kafka_subscribe(conf->rd_kafka_inst, conf->topic_partitions))) {
        fprintf(stderr,
                "%% Failed to start consuming topics: %s\n",
                rd_kafka_err2str(rd_err));
        rb_raise(rb_eRuntimeError, "Failed to start consuming topic %s", group_id);
        return Qnil;
    }
    conf->subscribed = 1;
    conf->running = 1;

    return rb_ensure(helper__consumer_loop, self, helper__consumer_loop_stop, self);
}
static VALUE elvan_consume_stop(VALUE self){
    Elvan_Config_t* conf;
    Data_Get_Struct(self, Elvan_Config_t, conf);
    conf->running = 0;
    return self;
}

void Init_elvan_rdkafka_consumer() {
    VALUE consumer_klass, rd_kafka_module;
    elvan_module = rb_define_module("Elvan");
    rd_kafka_module = rb_define_module_under(elvan_module, "RdKafka");
    consumer_klass = rb_define_class_under(rd_kafka_module, "Consumer", rb_cObject);

    rb_define_alloc_func(consumer_klass, elvan_consumer_allocate);
    rb_define_method(consumer_klass, "initialize", elvan_initialize, 2);
    rb_define_method(consumer_klass, "consume", elvan_consume, 0);
    rb_define_method(consumer_klass, "consume_stop!", elvan_consume_stop, 0);
}