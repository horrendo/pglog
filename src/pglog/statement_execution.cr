require "openssl"
require "uuid"
require "json"

module Pglog
  class StatementExecution
    include JSON::Serializable
    property id : String
    property log_time : Time
    property user_name : String
    property database_name : String
    property session_id : String
    property sql : String
    property transaction_id : Int64
    property time_parse_usec : Int32
    property time_bind_usec : Int32
    property time_fetch_usec : Int64
    property time_exec_usec : Int64
    property n_fetch : Int32
    property bind_parameters : String?
    property error : ExecutionError?
    property execute_fetch : Bool

    def self.sql_to_id(sql : String) : String
      return UUID.new(OpenSSL::MD5.hash(sql)).to_s
    end

    def initialize(raw : Raw, sql : String? = nil)
      @log_time = raw.log_time
      @user_name = raw.user_name
      @database_name = raw.database_name
      @session_id = raw.session_id
      @transaction_id = raw.transaction_id
      if sql
        @sql = sql
      elsif md = raw.message.match(/^statement[:]\s+(?<sql>.*)/m)
        @sql = md["sql"]
      else
        @sql = "?"
      end
      @id = StatementExecution.sql_to_id(@sql)
      @time_parse_usec = 0
      @time_bind_usec = 0
      @time_fetch_usec = 0
      @time_exec_usec = 0
      @n_fetch = 0
      @bind_parameters = nil
      @error = nil
      @execute_fetch = false
    end

    def set_bind(raw : Raw)
      unless @bind_parameters
        if md = raw.detail.match(/^parameters:\s+(?<bind>.*)/m)
          @bind_parameters = md["bind"]
        end
      end
    end
  end
end
