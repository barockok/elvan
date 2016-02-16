module Elvan
  class Consumer
    def initialize topics, options
      @rdkafka = Elvan::RdKafka::Consumer.new(topics, options)
    end

    def consume &block
      trap("SIGINT"){consume_stop!}
      @rdkafka.consume &block
    end

    def consume_stop!
      @rdkafka.consume_stop!
    end
  end
end