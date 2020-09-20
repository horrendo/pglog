require "json"

module Pglog
  class ExecutionError
    include JSON::Serializable
    property sql_state_code : String
    property message : String
    property detail : String
    property hint : String
    property internal_query : String
    property internal_query_pos : Int32?
    property context : String
    property query : String
    property query_pos : Int32?

    def initialize(raw : Raw)
      @sql_state_code = raw.sql_state_code
      @message = raw.message
      @detail = raw.detail
      @hint = raw.hint
      @internal_query = raw.internal_query
      @internal_query_pos = raw.internal_query_pos
      @context = raw.context
      @query = raw.query
      @query_pos = raw.query_pos
    end
  end
end
