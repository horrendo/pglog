require "../spec_helper"
require "./simple-handler"
require "./filter-handler"
require "./session-filter-handler"

describe Pglog do
  describe "EventHandler" do
    it "correctly processes before_row events" do
      csv = File.new("#{__DIR__}/csv/nothing-interesting.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.rows.should eq(reader.rows_processed)
      handler.rows.should eq(reader.rows_read)
      handler.n_connection.should eq(0)
      handler.n_disconnection.should eq(0)
      handler.n_statement.should eq(0)
      csv.close
    end

    it "correctly filters before_row events" do
      csv = File.new("#{__DIR__}/csv/nothing-interesting.csv")
      handler = MyFilteringEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      reader.rows_processed.should eq(0)
      csv.close
    end

    it "correctly processes new connections and disconnections" do
      csv = File.new("#{__DIR__}/csv/connection.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.n_connection.should eq(1)
      handler.n_disconnection.should eq(1)
      handler.n_statement.should eq(0)
      conn = handler.connections[0]
      conn.timestamp.should eq(Pglog::Spec.to_time("2020-05-02 05:01:58.444"))
      conn.user_name.should eq("b2bc")
      conn.database_name.should eq("postgres")
      conn.session_id.should eq("5eacfec6.2c")
      conn.duration.should eq(Time::Span.new(nanoseconds: 1_000_000))
      conn.executions.size.should eq(0)
      conn.last_execution.should eq(nil)
      conn.application_name.should eq("psql")
      disconn = handler.disconnections[0]
      disconn.timestamp.should eq(Pglog::Spec.to_time("2020-05-02 05:01:58.597"))
      disconn.user_name.should eq(conn.user_name)
      disconn.database_name.should eq(conn.database_name)
      disconn.session_id.should eq(conn.session_id)
      disconn.session_start_time.should eq(conn.timestamp)
      disconn.session_duration.should eq(Time::Span.new(nanoseconds: 153_000_000))
      csv.close
    end

    it "correctly processes simple statements" do
      csv = File.new("#{__DIR__}/csv/simple-statement.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.n_connection.should eq(0)
      handler.n_disconnection.should eq(0)
      handler.n_statement.should eq(1)
      stmt = handler.statements[0]
      stmt.log_time.should eq(Pglog::Spec.to_time("2020-05-03 05:44:16.886"))
      stmt.user_name.should eq("b2bc_owner")
      stmt.database_name.should eq("b2bc_dev")
      stmt.session_id.should eq("5eae5991.c3ad")
      stmt.sql.should eq("select count(*) from invoice;")
      stmt.transaction_id.should eq(1234)
      stmt.time_parse_usec.should eq(0)
      stmt.time_bind_usec.should eq(0)
      stmt.time_fetch_usec.should eq(0)
      stmt.time_exec_usec.should eq(6047)
      stmt.n_fetch.should eq(0)
      stmt.bind_parameters.should eq(nil)
      stmt.error.should eq(nil)
      reader.rows_read.should be > 0
      csv.close
    end

    it "correctly processes a simple error" do
      csv = File.new("#{__DIR__}/csv/simple-error.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.n_statement.should eq(1)
      stmt = handler.statements[0]
      stmt.log_time.should eq(Pglog::Spec.to_time("2020-05-09 05:25:15.351"))
      stmt.sql.should eq("select x;")
      stmt.error.should be_truthy
      if error = stmt.error
        error.sql_state_code.should eq("42703")
        error.message.should eq(%(column "x" does not exist))
        error.detail.should eq("")
        error.hint.should eq("")
        error.internal_query.should eq("")
        error.internal_query_pos.should be_nil
        error.context.should eq("")
        error.query.should eq(stmt.sql)
        error.query_pos.should eq(8)
      end
      csv.close
    end

    it "correctly processes a less-simple error" do
      csv = File.new("#{__DIR__}/csv/less-simple-error.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.n_statement.should eq(1)
      stmt = handler.statements[0]
      stmt.log_time.should eq(Pglog::Spec.to_time("2020-05-09 06:16:36.990"))
      stmt.sql.should eq("select cause_error();")
      stmt.error.should be_truthy
      if error = stmt.error
        error.sql_state_code.should eq("42P01")
        error.message.should eq(%(relation "x" does not exist))
        error.detail.should eq("")
        error.hint.should eq("")
        error.internal_query.should eq("select x          from x")
        error.internal_query_pos.should eq(24)
        error.context.should eq("PL/pgSQL function cause_error() line 1 at SQL statement")
        error.query.should eq(stmt.sql)
        error.query_pos.should be_nil
      end
      csv.close
    end

    it "correctly processes a parse-bind-execute statement" do
      csv = File.new("#{__DIR__}/csv/parse-bind-exec.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.n_statement.should eq(1)
      stmt = handler.statements[0]
      stmt.log_time.should eq(Pglog::Spec.to_time("2020-05-02 05:21:13.695"))
      stmt.bind_parameters.should be_truthy
      if b = stmt.bind_parameters
        b.size.should be > 0
      end
      stmt.time_parse_usec.should eq(412)
      stmt.time_bind_usec.should eq(108)
      stmt.time_exec_usec.should eq(15905)
      reader.sessions.size.should eq(1)
      session = reader.sessions["5ead0349.ad6"]
      session.executions.size.should eq(0)
      session.last_execution.should be_nil
      csv.close
    end

    it "correctly processes a prepare-execute statement" do
      csv = File.new("#{__DIR__}/csv/prepare-execute.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.n_statement.should eq(11)
      statements = handler.statements
      statements[0].id.should eq("e5919698-0aa8-6bd0-1ac5-24781509e292")
      statements[0].time_exec_usec.should eq(229)
      statements[1].id.should eq("6ed37420-2010-5d42-e438-1016de2bb1a2")
      statements[1].time_parse_usec.should eq(221)
      statements[1].time_exec_usec.should eq(0)
      statements[2].id.should eq(statements[1].id)
      statements[2].time_parse_usec.should eq(0)
      statements[2].time_bind_usec.should eq(0)
      statements[2].time_exec_usec.should eq(1289)
      statements[2].bind_parameters.should eq("execute c('buyer', 'text', 5);")
      statements[10].id.should eq(statements[1].id)
      statements[10].time_parse_usec.should eq(0)
      statements[10].time_bind_usec.should eq(0)
      statements[10].time_exec_usec.should eq(751)
      statements[10].bind_parameters.should eq("execute c(substr('buyerx', 1, 5), 'te' || 'x' || 't', '12345'::int);")
      csv.close
    end

    it "correctly processes a session with many cursor fetches" do
      csv = File.new("#{__DIR__}/csv/mega-fetch.csv")
      handler = MySessionFilteringEventHandler.new("5f66bc04.14a35")
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      handler.n_rows.should eq(225)
      handler.n_processed.should eq(205)
      handler.n_skipped.should eq(20)
      handler.statements.size.should eq(2)
      handler.statements[0].time_parse_usec.should eq(39688)
      handler.statements[0].time_bind_usec.should eq(57061)
      handler.statements[0].time_exec_usec.should eq(226291)
      handler.statements[0].time_fetch_usec.should eq(0)
      handler.statements[0].n_fetch.should eq(0)
      handler.statements[1].time_parse_usec.should eq(0)
      handler.statements[1].time_bind_usec.should eq(0)
      handler.statements[1].time_exec_usec.should eq(0)
      handler.statements[1].time_fetch_usec.should eq(347093)
      handler.statements[1].n_fetch.should eq(100)
      csv.close
    end

    it "correctly serializes and deserializes a reader" do
      csv = File.new("#{__DIR__}/csv/serialize.csv")
      handler = MySimpleEventHandler.new
      reader = Pglog::Reader.new(handler)
      reader.read(csv)
      reader_s : String = reader.to_json
      reader_2 = Pglog::Reader.from_json(reader_s)
      reader_2.to_json.should eq(reader_s)
      csv.close
    end
  end
end
