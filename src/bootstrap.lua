local Bootstrap = {}

function Bootstrap.new(bootstrap_peers)
  for k, peer in ipairs(bootstrap_peers) do
    local peer_name, peer_port
    if type(peer) == "string" then
      peer_name, peer_port = peer:match("(.*):(%d+)$")
      if not peer_name then
        peer_name, peer_port = peer, 7075
      else
        peer_port = tonumber(peer_port)
      end
    else
      peer_name, peer_port = peer[1] or peer.name or peer.host, peer[2] or peer.port
    end
    
    
    
  end
end



return Bootstrap
