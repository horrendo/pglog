# require "./spec_helper"

class MySessionFilteringEventHandler < Pglog::EventHandler
  getter statements = [] of Pglog::StatementExecution
  getter n_rows = 0
  getter n_processed = 0
  getter n_skipped = 0

  def initialize(@session_id : String)
  end

  def before_row(raw : Pglog::Raw) : Bool
    @n_rows += 1
    if processing = (raw.session_id == @session_id)
      @n_processed += 1
    else
      @n_skipped += 1
    end
    return processing
  end

  def on_statement(statement : Pglog::StatementExecution)
    @statements.push(statement)
  end
end
