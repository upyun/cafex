defmodule Cafex.Protocol.Compression do

  @spec compress(binary, :gzip | :snappy) :: binary
  def compress(data, :gzip) do
    :zlib.gzip(data)
  end
  def compress(data, :snappy) do
    {:ok, bin} = :snappy.compress(data)
    bin
  end

  @spec decompress(binary, :gzip | :snappy) :: binary
  def decompress(data, :gzip) do
    :zlib.gunzip(data)
  end
  def decompress(data, :snappy) do
    << _snappy_header :: 64, _snappy_version_info :: 64, rest :: binary>> = data
    snappy_decompress_chunk(rest, <<>>)
  end

  defp snappy_decompress_chunk(<<>>, data), do: data
  defp snappy_decompress_chunk(<< valsize :: 32-unsigned,
                              value :: size(valsize)-binary,
                              rest :: binary>>, data) do
    {:ok, decompressed_value} = :snappy.decompress(value)
    snappy_decompress_chunk(rest, data <> decompressed_value)
  end
end
