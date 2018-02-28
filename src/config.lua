local cfg = {
  node = {
    peering_port = 7075,
    preconfigured_peers = {
      "rai.raiblocks.net"
    },
    max_peers = 200
  },
  bootstrap = {
    min_frontier_size = 430000
  }
}

return cfg
