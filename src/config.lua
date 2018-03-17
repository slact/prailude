local cfg = {
  node = {
    peering_port = 7075,
    preconfigured_peers = {
      "rai.raiblocks.net"
    },
    max_peers = 700
  },
  bootstrap = {
    min_frontier_size = 430000,
    max_peers = 150,
    
  }
}

return cfg
