# require "./spec_helper"

class MySimpleEventHandler < Pglog::EventHandler
  getter rows : Int64 = 0
  getter connections = [] of Pglog::Connection
  getter disconnections = [] of Pglog::Disconnection
  getter statements = [] of Pglog::StatementExecution

  def before_row(raw : Pglog::Raw) : Bool
    @rows += 1
    return true
  end

  def on_connect(connection : Pglog::Connection)
    @connections.push(connection)
  end

  def on_statement(statement : Pglog::StatementExecution)
    @statements.push(statement)
  end

  def on_disconnect(disconnection : Pglog::Disconnection)
    @disconnections.push(disconnection)
  end

  def n_connection : Int32
    @connections.size
  end

  def n_disconnection : Int32
    @disconnections.size
  end

  def n_statement : Int32
    @statements.size
  end
end
