defmodule Cafex.Util do

  @app :cafex

  def get_config(opts, key, default \\ nil) do
    case Keyword.get(opts, key) do
      nil -> Application.get_env(@app, key, default)
      val -> val
    end
  end
end
