require "json"

module Pglog
  class Raw
    include JSON::Serializable
    property log_time : Time
    property user_name : String
    property database_name : String
    property process_id : Int32
    property connection_from : String
    property session_id : String
    property session_line_num : Int64
    property command_tag : String
    property session_start_time : Time
    property virtual_transaction_id : String
    property transaction_id : Int64
    property error_severity : String
    property sql_state_code : String
    property message : String
    property detail : String
    property hint : String
    property internal_query : String
    property internal_query_pos : Int32?
    property context : String
    property query : String
    property query_pos : Int32?
    property location : String
    property application_name : String

    def initialize(row : CSV::Row)
      log_time, @user_name, @database_name, process_id, @connection_from, @session_id, session_line_num, @command_tag, session_start_time, @virtual_transaction_id, transaction_id, @error_severity, @sql_state_code, @message, @detail, @hint, @internal_query, internal_query_pos, @context, @query, query_pos, @location, @application_name = row.to_a

      @log_time = Time.parse(log_time, "%F %T.%L", Time::Location::UTC)
      @process_id = process_id.to_i32
      @session_line_num = session_line_num.to_i64
      @session_start_time = Time.parse(session_start_time, "%F %T", Time::Location::UTC)
      @transaction_id = transaction_id.to_i64
      @internal_query_pos = internal_query_pos.to_i32 if internal_query_pos.size > 0
      @query_pos = query_pos.to_i32 if query_pos.size > 0
    end
  end
end
