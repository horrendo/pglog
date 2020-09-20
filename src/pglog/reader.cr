require "csv"
require "json"
require "pg_sql_lexer"

module Pglog
  class Reader
    alias Normaliser = Proc(String, String)

    include JSON::Serializable
    property rows_read : Int64
    property rows_processed : Int64
    property pending_connections : Hash(String, Raw)
    property sessions : Hash(String, Connection)
    @[JSON::Field(ignore: true)]
    property handler : EventHandler | Nil

    def initialize(@handler : EventHandler)
      @rows_read = 0
      @rows_processed = 0
      @pending_connections = Hash(String, Raw).new
      @sessions = Hash(String, Connection).new
    end

    def read(stream : IO)
      CSV.new(stream) do |csv|
        @rows_read += 1
        raw = Raw.new(csv.row)
        next unless assert_handler.before_row(raw)
        if raw.session_line_num == 1 && raw.connection_from.size > 0
          @pending_connections[raw.session_id] = raw
          next
        end
        if raw.session_line_num == 2 && @pending_connections.has_key?(raw.session_id)
          process_connection(raw)
        elsif raw.command_tag == "idle" && raw.message.starts_with?("disconnection: session time: ")
          process_disconnection(raw)
        elsif raw.command_tag == "idle" && raw.message.starts_with?("statement: ")
          process_statement(raw)
        elsif raw.error_severity == "ERROR"
          process_error(raw)
        elsif raw.message.starts_with?("duration: ")
          process_duration(raw)
        elsif raw.message.starts_with?("execute ")
          process_execute(raw)
        end
        @rows_processed += 1
      end
    end

    private def assert_handler : EventHandler
      @handler || raise "handler must be defined"
    end

    private def process_statement(raw : Raw)
      session = @sessions[raw.session_id]? || process_connection(raw, false)
      if md = raw.message.match(/^statement[:]\s+(?<sql>.*)/mi)
        tokens = PgSqlLexer::Lexer.new(md["sql"]).tokens
        case tokens[0].value.try &.downcase
        when "prepare"
          find_or_create_execution(session, raw, sql_for_prepare(tokens))
        when "execute"
          if dtl_md = raw.detail.match(/^prepare[:]\s+(?<sql>.*)/mi)
            prepare_tokens = PgSqlLexer::Lexer.new(dtl_md["sql"]).tokens
            exec = find_or_create_execution(session, raw, sql_for_prepare(prepare_tokens))
            exec.bind_parameters = PgSqlLexer::Formatter.new(tokens).format_minified
          end
        else
          find_or_create_execution(session, raw, PgSqlLexer::Formatter.new(tokens).format_minified)
        end
      end
    end

    private def process_error(raw : Raw)
      session = @sessions[raw.session_id]? || process_connection(raw, false)
      if last_execution = session.last_execution
        last_execution.error = ExecutionError.new(raw)
        on_last_statement(session)
      end
    end

    private def process_execute(raw : Raw)
      session = @sessions[raw.session_id]? || process_connection(raw, false)
      if md = raw.message.match(/^execute\s+(?<what>[^:]*)[:]?\s*(?<sql>.*)/m)
        sql = normalise(md["sql"])
        last_execution = find_or_create_execution(session, raw, sql)
        if md["what"].starts_with?("fetch from ")
          last_execution.execute_fetch = true
          last_execution.n_fetch += 1
        end
      end
    end

    private def process_duration(raw : Raw)
      session = @sessions[raw.session_id]? || process_connection(raw, false)
      if md = raw.message.match(/^duration:\s+(?<duration>\S+) ms\s*(?<verb>\S*)[^:]*[:]?\s*(?<sql>.*)/m)
        duration : Int64 = (md["duration"].to_f * 1000).to_i64
        last_execution : StatementExecution
        if md["verb"].size == 0
          if e = session.last_execution
            last_execution = e
          else
            return
          end
        else
          sql = normalise(md["sql"])
          last_execution = find_or_create_execution(session, raw, sql)
        end
        case raw.command_tag
        when "PARSE"
          last_execution.time_parse_usec = duration.to_i32
        when "PREPARE"
          last_execution.time_parse_usec = duration.to_i32
          on_last_statement(session)
        when "BIND"
          last_execution.time_bind_usec = duration.to_i32
          last_execution.set_bind(raw)
        when "SELECT"
          if last_execution.execute_fetch
            last_execution.time_fetch_usec += duration
            last_execution.execute_fetch = false
          else
            last_execution.time_exec_usec = duration
            on_last_statement(session)
          end
        else
          last_execution.time_exec_usec = duration
          on_last_statement(session)
        end
      end
    end

    private def sql_for_prepare(tokens : Array(PgSqlLexer::Token)) : String
      while tokens[0].value.try &.downcase != "as"
        tokens.shift
      end
      tokens.shift
      PgSqlLexer::Formatter.new(tokens).format_minified
    end

    private def on_last_statement(session : Connection)
      if exec = session.last_execution
        assert_handler.on_statement(exec)
        session.last_execution = nil
        session.executions.delete(exec.id)
      end
    end

    private def find_or_create_execution(session : Connection, raw : Raw, sql : String) : StatementExecution
      id = StatementExecution.sql_to_id(sql)
      if (e = session.last_execution) && e.id != id
        on_last_statement(session)
      end
      last_execution : StatementExecution? = nil
      if session.executions.has_key?(id)
        last_execution = session.executions[id]
      else
        last_execution = StatementExecution.new(raw, sql)
        last_execution.set_bind(raw)
        session.executions[last_execution.id] = last_execution
      end
      session.last_execution = last_execution
      return last_execution
    end

    private def process_connection(raw : Raw, fire_event = true) : Connection
      connection = Connection.new(raw, @pending_connections)
      assert_handler.on_connect(connection) if fire_event
      @pending_connections.delete(raw.session_id)
      @sessions[raw.session_id] = connection
    end

    private def process_disconnection(raw : Raw)
      session = @sessions[raw.session_id]? || process_connection(raw, false)
      if session.last_execution
        on_last_statement(session)
      end
      disconnection = Disconnection.new(raw, @sessions[raw.session_id]?.try &.timestamp)
      assert_handler.on_disconnect(disconnection)
      @sessions.delete(raw.session_id)
    end

    private def normalise(raw_sql : String) : String
      PgSqlLexer::Formatter.new(PgSqlLexer::Lexer.new(raw_sql).tokens).format_minified
    end
  end
end
