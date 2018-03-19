typedef enum {
  NANO_MSG_INVALID =       0,
  NANO_MSG_NO_TYPE,      //1
  NANO_MSG_KEEPALIVE,    //2
  NANO_MSG_PUBLISH,      //3
  NANO_MSG_CONFIRM_REQ,  //4
  NANO_MSG_CONFIRM_ACK,  //5
  NANO_MSG_BULK_PULL,    //6
  NANO_MSG_BULK_PUSH,    //7
  NANO_MSG_FRONTIER_REQ, //8
  NANO_MSG_BULK_PULL_BLOCKS //9
} nano_msg_type_t;

typedef enum {
  NANO_BLOCK_INVALID =     0,
  NANO_BLOCK_NOT_A_BLOCK,//1
  NANO_BLOCK_SEND,       //2
  NANO_BLOCK_RECEIVE,    //3
  NANO_BLOCK_OPEN,       //4
  NANO_BLOCK_CHANGE      //5
} nano_block_type_t;

typedef enum {
  NANO_TESTNET = 0,
  NANO_BETANET,
  NANO_MAINNET
} nano_network_type_t;

typedef struct {
  nano_network_type_t  net;          //first 2 chars, "RA", "RB", or "RC"
  uint8_t              version_max;
  uint8_t              version_cur;
  uint8_t              version_min;
  nano_msg_type_t      msg_type;     //1 char
  uint8_t              extensions;
  nano_block_type_t    block_type;   //1 char
} nano_msg_header_t;
