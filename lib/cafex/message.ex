defmodule Cafex.Message do
  defstruct topic: nil,
              key: nil,
            value: nil

  @type t :: %Cafex.Message{}
end
