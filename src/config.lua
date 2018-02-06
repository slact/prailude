local cfg = {
  node = {
    peering_port = 7075,
    preconfigured_peers = {
      "rai.raiblocks.net"
    }
  },
  bootstrap = {
    min_frontier_size = 335700
  }
}

return cfg
