Application.start :erlzk

Code.require_file("support/zk_helper.exs", __DIR__)
Logger.remove_backend(:console)

zk_cfg = Application.get_env(:cafex, :zookeeper)
zk_prefix  = Keyword.get(zk_cfg, :path)
{:ok, pid} = ZKHelper.connect(zk_cfg)
:ok = ZKHelper.rmr(pid, zk_prefix)
# ZKHelper.close(pid)

ExUnit.start()
