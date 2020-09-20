# require "./spec_helper"

class MyFilteringEventHandler < Pglog::EventHandler
  getter n_connections : Int32 = 0
  getter n_disconnections : Int32 = 0

  def before_row(raw : Pglog::Raw) : Bool
    return false
  end

  def on_connect(connection : Pglog::Connection)
    @n_connections += 1
  end

  def on_disconnect(disconnection : Pglog::Disconnection)
    @n_disconnections += 1
  end
end
