local cfg = {
  node = {
    peering_port = 7075,
    preconfigured_peers = {
      "rai.raiblocks.net"
    },
    max_peers = 100
  },
  bootstrap = {
    min_frontier_size = 335700
  }
}

return cfg
