#define INET6_ADDRSTRLEN 46
#define INET_ADDRSTRLEN 16
int inet_ntop4(const unsigned char *src, char *dst, size_t size);
int inet_ntop6(const unsigned char *src, char *dst, size_t size);

int inet_pton6(const char *src, unsigned char *dst);
int inet_pton4(const char *src, unsigned char *dst);
