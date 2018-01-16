local Transaction_instance = {
  
}

local tx_meta = {__index = Transaction_instance}

local Transaction = {
  new = function(data)
    return setmetatable(data, tx_meta)
  end
}


return Transaction
