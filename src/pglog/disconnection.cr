module Pglog
  class Disconnection
    getter timestamp : Time
    getter user_name : String
    getter database_name : String
    getter session_id : String
    getter session_start_time : Time
    getter session_duration : Time::Span

    def initialize(raw : Raw, connect_time : Time?)
      @timestamp = raw.log_time
      @user_name = raw.user_name
      @database_name = raw.database_name
      @session_id = raw.session_id
      @session_start_time = connect_time || raw.session_start_time
      if md = raw.message.match(/ session time: (?<hh>\d+)[:](?<mm>\d\d)[:](?<ss>\d\d)[.](?<ms>\d+)/)
        @session_duration = Time::Span.new(hours: md["hh"].to_i, minutes: md["mm"].to_i, seconds: md["ss"].to_i, nanoseconds: md["ms"].to_i * 1_000_000)
      else
        @session_duration = Time::Span.new(seconds: 0)
      end
    end
  end
end
