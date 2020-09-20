module Pglog
  abstract class EventHandler
    def before_row(raw : Raw) : Bool
      true
    end

    def on_statement(statement : StatementExecution)
    end

    def on_connect(connection : Connection)
    end

    def on_disconnect(disconnection : Disconnection)
    end
  end
end
