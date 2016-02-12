# Elvan

Usage
```ruby
require 'elvan'
# argument for initialize
# 0 : broker list
# 1 : Topic list
# 2 : hash options
#   - group_id
el = Elvan::Consumer.new(['localhost:9092'], ['transactions'], group_id: 'holla-holla')
el.consume do |message, key, offset|
  puts "message : #{} | key : #{key} | offset : #{offset}"
end
```