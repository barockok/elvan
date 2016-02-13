# Elvan

Usage
```ruby
require 'elvan'
el = Elvan::RdKafka::Consumer.new(["some-topic-name"], "group.id" => "group-id-name", "bootstrap.servers" => "localhost:9092"})
el.consume do |message, key, offset|
  puts "message : #{message} | key : #{key} | offset : #{offset}"
end
```