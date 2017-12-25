typedef enum {
  RAI_MSG_INVALID =       0,
  RAI_MSG_NO_TYPE,      //1
  RAI_MSG_KEEPALIVE,    //2
  RAI_MSG_PUBLISH,      //3
  RAI_MSG_CONFIRM_REQ,  //4
  RAI_MSG_CONFIRM_ACK,  //5
  RAI_MSG_BULK_PULL,    //6
  RAI_MSG_BULK_PUSH,    //7
  RAI_MSG_FRONTIER_REQ  //8
} rai_msg_type_t;

typedef enum {
  RAI_BLOCK_INVALID =     0,
  RAI_BLOCK_NOT_A_BLOCK,//1
  RAI_BLOCK_SEND,       //2
  RAI_BLOCK_RECEIVE,    //3
  RAI_BLOCK_OPEN,       //4
  RAI_BLOCK_CHANGE      //5
} rai_block_type_t;

typedef enum {
  RAI_TESTNET = 0,
  RAI_BETANET,
  RAI_MAINNET
} rai_network_type_t;

typedef struct {
  rai_network_type_t  net;          //first 2 chars, "RA", "RB", or "RC"
  uint8_t             version_max;
  uint8_t             version_cur;
  uint8_t             version_min;
  rai_msg_type_t      msg_type;     //1 char
  uint8_t             extensions;
  rai_block_type_t    block_type;   //1 char
} rai_msg_header_t;
