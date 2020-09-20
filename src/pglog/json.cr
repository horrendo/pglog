require "json"

module Pglog
  struct TimeSpanConverter
    def self.from_json(parser : JSON::PullParser) : Time::Span
      if val = parser.read?(Int64)
        return Time::Span.new(nanoseconds: val)
      end
      raise "Error deserializing Time::Span"
    end

    def self.to_json(span : Time::Span, builder : JSON::Builder)
      builder.number(span.total_nanoseconds.to_i64)
    end
  end
end
