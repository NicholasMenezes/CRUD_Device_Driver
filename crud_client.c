////////////////////////////////////////////////////////////////////////////////
//
//  File          : crud_client.c
//  Description   : This is the client side of the CRUD communication protocol.
//
//   Author       : Ryan Geiger 
//  Last Modified : Thu Oct 30 06:59:59 EDT 2014
//

// Include Files

// Project Include Files
#include <crud_network.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

// Global variables
int            crud_network_shutdown = 0; // Flag indicating shutdown
unsigned char *crud_network_address = NULL; // Address of CRUD server 
unsigned short crud_network_port = 0; // Port of CRUD server
int            socket_fd = -1; // socket file descriptor

//
// Functions

int crud_send(CrudRequest request, void *buf);
CrudResponse crud_receive(void *buf);

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_client_operation
// Description  : This the client operation that sends a request to the CRUD
//                server.   It will:
//
//                1) if INIT make a connection to the server
//                2) send any request to the server, returning results
//                3) if CLOSE, will close the connection
//
// Inputs       : op - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : the response structure encoded as needed

CrudResponse crud_client_operation(CrudRequest op, void *buf) {
    // Declare variables
    CrudResponse response;
    uint8_t req;

    // Extract the request type
    req = (uint8_t) ((op >> 28) & 0xf);

    // if CRUD_INIT then make a connection to the server 
    if (req == CRUD_INIT)
    {
        // Create socket
        socket_fd = socket(PF_INET, SOCK_STREAM, 0);
        if (socket_fd == -1)
        {
            printf("Error on socket creation\n");
            return(-1);
        }

        // Specify address to connect to
        struct sockaddr_in v4;
        v4.sin_family = AF_INET;
        v4.sin_port = htons(CRUD_DEFAULT_PORT);
        if (inet_aton(CRUD_DEFAULT_IP, &(v4.sin_addr)) == 0)
        {
            return(-1);
        }

        // Connect
        if (connect(socket_fd, (const struct sockaddr *)&v4,
                    sizeof(struct sockaddr)) == -1)
        {
            printf("Error connecting to server\n");
            return(-1);
        }
    }

    // Send request to server
    if (crud_send(op, buf) != 0)
        return -1;

    // Receive response
    response = crud_receive(buf);

    // if CRUD_CLOSE, close the connection
    if (req == CRUD_CLOSE) 
    {
        close(socket_fd);
        socket_fd = -1;
    }

    return response;
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_send
// Description  : This is the function that sends the client CrudRequest to the
//                  server (and buffer if necessary).
//
// Inputs       : request - the request opcode for the command
//                buf - the block to be read/written from (READ/WRITE)
// Outputs      : 0 if successful, -1 if error 

int crud_send(CrudRequest request, void *buf)
{
    // Declare variables
    int request_length = sizeof(CrudRequest), request_written;
    CrudRequest *request_network_order = malloc(request_length);
    int req = ((request >> 28) & 0xf);
    int buf_length = ((request >> 4) & 0xffffff), buf_written;

    // Convert request value to network byte order 
    *request_network_order = htonll64(request);

    // Send converted request value, make sure all bytes are sent
    request_written = write(socket_fd, request_network_order, request_length); 
    while (request_written < request_length)
    {
        request_written += write(socket_fd, &((char *)request_network_order)[request_written],
                request_length - request_written);
    }
    free(request_network_order);

    // Check if you need to send buffer as well
    if (req == CRUD_CREATE || req == CRUD_UPDATE)
    {
        buf_written = write(socket_fd, buf, buf_length);
        while (buf_written < buf_length)
        {
            buf_written += write(socket_fd, &((char *)buf)[buf_written], buf_length - buf_written);
        }
    }

    return 0;
}


////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_receive
// Description  : This is the function that receives the server response (and
//                  buffer if necessary).
//
// Inputs       : buf - the block to be read/written from (READ/WRITE)
// Outputs      : server CrudResponse

CrudResponse crud_receive(void *buf)
{
    // Declare variables
    CrudResponse response_host_order;
    int response_length = sizeof(CrudResponse), response_read;
    CrudResponse *response = malloc(response_length);
    int buf_length, buf_read;
    int response_req;

    // Receive response value
    response_read = read(socket_fd, response, response_length);
    while (response_read < response_length)
    {
        response_read += read(socket_fd, &((char *)response)[response_read], response_length - response_read);
    }

    // Convert received value into host byte order
    response_host_order = ntohll64(*response);
    free(response);

    // Extract request type and length from converted response
    response_req = ((response_host_order >> 28) & 0xf);
    buf_length = ((response_host_order >> 4) & 0xffffff);

    // Check if you need to receive buffer
    if (response_req == CRUD_READ)
    {
        buf_read = read(socket_fd, buf, buf_length);
        while (buf_read < buf_length)
        {
            buf_read += read(socket_fd, &((char *)buf)[buf_read], buf_length - buf_read);
        }
    }

    return response_host_order;
}
