defmodule Cafex.Util do

  def get_config(opts, fallback, key, default \\ nil) do
    case Keyword.get(opts, key) do
      nil -> Keyword.get(fallback, key, default)
      val -> val
    end
  end
end
