require "uuid"
require "json"

module Pglog
  class Connection
    include JSON::Serializable
    property timestamp : Time
    property database_name : String
    property user_name : String
    property session_id : String
    @[JSON::Field(converter: Pglog::TimeSpanConverter)]
    property duration : Time::Span?
    property executions : Hash(String, StatementExecution)
    property last_execution : StatementExecution?
    property application_name : String

    def initialize(raw : Raw, pending : Hash(String, Raw))
      @timestamp = raw.log_time
      @database_name = raw.database_name
      @user_name = raw.user_name
      @session_id = raw.session_id
      if md = raw.message.match(/ application_name=(?<app>.*)/)
        @application_name = md["app"]
      else
        @application_name = raw.application_name
      end
      if p = pending[raw.session_id]?
        @timestamp = p.log_time
        @duration = raw.log_time - p.log_time
      elsif raw.session_line_num == 2
        @timestamp = raw.log_time
      else
        @timestamp = raw.session_start_time
      end
      @executions = Hash(String, StatementExecution).new
    end
  end
end
