////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io.c
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the CRUD storage system.
//
//  Author         : Ryan Geiger 
//  Last Modified  : Sunday November 9 5:00:00 EST 2014
//

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <crud_file_io.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>
#include <crud_network.h>

// Global Variables
int InitFlag = 0;  // Set to 1 once CRUD_INIT request is called

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240

// Other definitions

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;

// File system Static Data
// This the definition of the file table
CrudFileAllocationType crud_file_table[CRUD_MAX_TOTAL_FILES]; // The file handle table

// Pick up these definitions from the unit test of the crud driver
CrudRequest construct_crud_request(CrudOID oid, CRUD_REQUEST_TYPES req,
		uint32_t length, uint8_t flags, uint8_t res);
int deconstruct_crud_request(CrudRequest request, CrudOID *oid,
		CRUD_REQUEST_TYPES *req, uint32_t *length, uint8_t *flags,
		uint8_t *res);

// Type for parameters of CrudRequest and CrudResponse
typedef struct {
    uint32_t oID;       // Object ID
    uint8_t req;        // Command or Request type
    uint32_t length;    // Size of object
    uint8_t flags;
    uint8_t res;        // Result - 0 if success, 1 if failure
} CRParsed;

//
// Implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : convert_to_CrudRequest
// Description  : This function converts five separate fields needed for a 
//                CrudRequest and combines them into a single 64-bit CrudRequest
//                value
//
// Inputs       : oID - 32-bit object identifier of the object being executed on
//                req - 4-bit request type of the command being executed 
//                length - 24-bit length object or buffer
//                flags - 3-bits, unused for now
//                res - 1-bit result code, 0 signifies success of command execution,
//                    1 failure
// Outputs      : 64-bit CrudRequest value 

CrudRequest convert_to_CrudRequest(uint32_t oID, uint8_t req, uint32_t length, 
        uint8_t flags, uint8_t res){

    // Initialize
    CrudRequest convertedValue = 0;
    uint32_t bitWork = 0;

    // Wipe extra bits of each parameter
    bitWork = (~bitWork) >> 8; // gives 1 in 24 least significant bits 
    length = length & bitWork; // only take 24 least signficant bits of length
    req = req & 15;            // only takes 4 least signficant bits of req
    flags = flags & 7;         // only takes 3 least signficant bits of flags
    res = res & 1;                 // only takes least significant bit of r

    // Shift parameters into proper positions in convertedValue
    convertedValue = (CrudRequest) oID << 32;
    convertedValue = convertedValue | ((CrudRequest) req << 28);
    convertedValue = convertedValue | ((CrudRequest) length << 4);
    convertedValue = convertedValue | ((CrudRequest) flags << 1);
    convertedValue = convertedValue | (CrudRequest) res;

    return convertedValue;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : parse_CrudResponse
// Description  : This function parses 64-bit CrudResponse value for five   
//                different fields
//
// Inputs       : parseThis - 64-bit CrudResponse value to be parsed 
// Outputs      : CRParsed structure containing five fields 

CRParsed parse_CrudResponse(CrudResponse parseThis) {
    // Initialize
    CRParsed parsedValues;
    uint32_t lengthBits = 0;
    lengthBits = (~lengthBits) >> 8; // Obtain 1 in 24 least significant bits 

    // Parse parseThis for each of five fields 
    parsedValues.oID = (uint32_t) (parseThis >> 32); // obtain bits 32-63 
    parsedValues.req = (uint8_t) ((parseThis >> 28) & 15); // obtain bits 28-31 
    parsedValues.length = (uint32_t) ((parseThis >> 4) & lengthBits); // obtain bits 4-27 
    parsedValues.flags = (uint8_t) ((parseThis >> 1) & 7); // obtain bits 1-3 
    parsedValues.res = (uint8_t) (parseThis & 1); // obtain bit 0 

    return parsedValues;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_format
// Description  : This function formats the crud drive, and adds the file
//                allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_format(void) {
    // Declare variables
    int i;

    // Initialize
    if (InitFlag == 0)
    {
        // Obtain CRUD_INIT CrudRequest code and pass to crud_client_operation
        CrudRequest initialize = convert_to_CrudRequest(0, CRUD_INIT, 0, 0, 0); 
        CrudResponse initialized = crud_client_operation(initialize, NULL);
        CRParsed initParsed = parse_CrudResponse(initialized);
        // Check if CRUD_INIT was successful
        if (initParsed.res == 1)
            return -1;

        InitFlag = 1;
    }

    // Format
    CrudRequest format = convert_to_CrudRequest(0, CRUD_FORMAT, 0, CRUD_NULL_FLAG, 0);
    CrudResponse formatted = crud_client_operation(format, NULL);
    CRParsed formatParsed = parse_CrudResponse(formatted);
    // Check if CRUD_FORMAT was successful
    if (formatParsed.res == 1)
        return -1;

    // Initialize file allocation table with zeros (signifying slots are unused)
    for (i = 0; i < CRUD_MAX_TOTAL_FILES; i++)
    {
        strcpy(crud_file_table[i].filename, "");
        crud_file_table[i].object_id = 0;
        crud_file_table[i].position = 0;
        crud_file_table[i].length = 0;
        crud_file_table[i].open = 0;
    }

    // Create priority object storing file allocation table
    CrudRequest create = convert_to_CrudRequest(0, CRUD_CREATE, 
            CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType), CRUD_PRIORITY_OBJECT, 0);
    CrudResponse created = crud_client_operation(create, crud_file_table);
    CRParsed createParsed = parse_CrudResponse(created);
    // Check if CRUD_CREATE was successful
    if (createParsed.res == 1)
        return -1;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... formatting complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_mount
// Description  : This function mount the current crud file system and loads
//                the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_mount(void) {
    // Initialize
    if (InitFlag == 0)
    {
        // Obtain CRUD_INIT CrudRequest code and pass to crud_client_operation
        CrudRequest initialize = convert_to_CrudRequest(0, CRUD_INIT, 0, 0, 0); 
        CrudResponse initialized = crud_client_operation(initialize, NULL);
        CRParsed initParsed = parse_CrudResponse(initialized);
        // Check if CRUD_INIT was successful
        if (initParsed.res == 1)
            return -1;

        InitFlag = 1;
    }

    // Read priority object, load file allocation table 
    CrudRequest read = convert_to_CrudRequest(0, CRUD_READ, 
            CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType), CRUD_PRIORITY_OBJECT, 0);
    CrudResponse readResponse = crud_client_operation(read, crud_file_table);
    CRParsed parsedReadResponse = parse_CrudResponse(readResponse);
    // Check if CRUD_READ was successful
    if (parsedReadResponse.res == 1)
        return -1;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... mount complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_unmount
// Description  : This function unmounts the current crud file system and
//                saves the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_unmount(void) {
    // Check that CRUD_INIT has already been called
    if (InitFlag == 0)
        return -1;

    // Update priority object with contents of file allocation table
    CrudRequest update = convert_to_CrudRequest(0, CRUD_UPDATE, 
            CRUD_MAX_TOTAL_FILES*sizeof(CrudFileAllocationType), CRUD_PRIORITY_OBJECT, 0);
    CrudResponse updated = crud_client_operation(update, crud_file_table);
    CRParsed updatedObject = parse_CrudResponse(updated);
    // Check if CRUD_UPDATE was successful
    if (updatedObject.res == 1)
        return -1;
    
    // Issue CRUD_CLOSE request to write to state file and
    //  shut down virtual hardware
    CrudRequest close = convert_to_CrudRequest(0, CRUD_CLOSE, 0, CRUD_NULL_FLAG, 0);
    CrudResponse closed = crud_client_operation(close, NULL);
    CRParsed parsedCloseResponse = parse_CrudResponse(closed);
    // Check if CRUD_CLOSE was successful
    if (parsedCloseResponse.res == 1)
        return -1;

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... unmount complete.");
	return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_open
// Description  : This function opens the file and returns a file handle
//
// Inputs       : path - the path "in the storage array"
// Outputs      : file handle if successful, -1 if failure

int16_t crud_open(char *path) {
    // Initialize variables
    int fh = 0;

    // Check if CRUD_INIT request has been called
    if (InitFlag == 0)
    {
        // Obtain CRUD_INIT CrudRequest code and pass to crud_client_operation
        CrudRequest initialize = convert_to_CrudRequest(0, CRUD_INIT, 0, 0, 0); 
        CrudResponse initialized = crud_client_operation(initialize, NULL);
        CRParsed initParsed = parse_CrudResponse(initialized);
        // Check if CRUD_INIT was successful
        if (initParsed.res == 1)
            return -1;

        InitFlag = 1;
    }

    // Validate parameters
    if (strlen(path) > CRUD_MAX_PATH_LENGTH || strlen(path) <= 0)
        return -1;

    // Search for filename in table
    while (fh < CRUD_MAX_TOTAL_FILES && strcmp(crud_file_table[fh].filename, path) != 0)
    {
        fh++;
    }

    // Check if file does not exist 
    if (fh == CRUD_MAX_TOTAL_FILES)
    {
        // Assign file new slot in crud_file_table

        fh = 0;
        // Search through file table until an empty slot is found
        while (strcmp(crud_file_table[fh].filename, "") != 0)
            fh++;

        // Check to make sure table is not full
        if (fh == CRUD_MAX_TOTAL_FILES)
            return -1;
        // Copy path into table filename 
        strcpy(crud_file_table[fh].filename, path);
        
        // Set initial contents to empty
        crud_file_table[fh].object_id = 0;
        crud_file_table[fh].position = 0;
        crud_file_table[fh].length = 0;
        crud_file_table[fh].open = 1;
    }
    // else file does already exist
    else
    {
        // Check to make sure file is not already open
        if (crud_file_table[fh].open == 1)
            return -1;

        // Open file
        // Set position = 0, open = 1
        crud_file_table[fh].position = 0;
        crud_file_table[fh].open = 1;
    }

    return fh; 
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_close
// Description  : This function closes the file
//
// Inputs       : fh - the file handle of the object to close
// Outputs      : 0 if successful, -1 if failure

int16_t crud_close(int16_t fh) {
    // Check if CRUD_INIT request has been called
    if (InitFlag == 0)
    {
        // Obtain CRUD_INIT CrudRequest code and pass to crud_client_operation
        CrudRequest initialize = convert_to_CrudRequest(0, CRUD_INIT, 0, 0, 0); 
        CrudResponse initialized = crud_client_operation(initialize, NULL);
        CRParsed initParsed = parse_CrudResponse(initialized);
        // Check if CRUD_INIT was successful
        if (initParsed.res == 1)
            return -1;

        InitFlag = 1;       
    }

    // Validate parameters
    if (fh >= CRUD_MAX_TOTAL_FILES || fh < 0) 
        return -1;
    if (crud_file_table[fh].open == 0)
        return -1;

    // Close file
    crud_file_table[fh].open = 0;

    return 0;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_read
// Description  : Reads up to "count" bytes from the file handle "fh" into the
//                buffer  "buf".
//
// Inputs       : fd - the file descriptor for the read
//                buf - the buffer to place the bytes into
//                count - the number of bytes to read
// Outputs      : the number of bytes read or -1 if failures

int32_t crud_read(int16_t fd, void *buf, int32_t count) {
    // Check if CRUD_INIT request has been called
    if (InitFlag == 0)
    {
        // Obtain CRUD_INIT CrudRequest code and pass to crud_client_operation
        CrudRequest initialize = convert_to_CrudRequest(0, CRUD_INIT, 0, 0, 0); 
        CrudResponse initialized = crud_client_operation(initialize, NULL);
        CRParsed initParsed = parse_CrudResponse(initialized);
        // Check if CRUD_INIT was successful
        if (initParsed.res == 1)
            return -1;

        InitFlag = 1;       
    }      

    // Validate parameters
    if (fd >= CRUD_MAX_TOTAL_FILES || fd < 0) 
        return -1;
	if (count < 0)
		return -1;
    if (crud_file_table[fd].open == 0)
        return -1;

    // Read object
    CrudRequest read = convert_to_CrudRequest(crud_file_table[fd].object_id, CRUD_READ, crud_file_table[fd].length, 0, 0);
    char *readBuf = malloc(crud_file_table[fd].length); 
    CrudResponse readResponse = crud_client_operation(read, readBuf);
    CRParsed parsedReadResponse = parse_CrudResponse(readResponse);
    // Check if CRUD_READ was successful
    if (parsedReadResponse.res == 1)
    {
        free(readBuf);
        return -1;
    }
    
	// Copy bytes from readBuf at position into buf
	// If the number of bytes to be read is greater than bytes left in file,
	//  then just read as many as are available, otherwise read count bytes
	if (count > crud_file_table[fd].length - crud_file_table[fd].position)
		memcpy(buf, &readBuf[crud_file_table[fd].position], crud_file_table[fd].length - crud_file_table[fd].position);
	else
		memcpy(buf, &readBuf[crud_file_table[fd].position], count);
    // Free memory
    free(readBuf);

    // Update position and return number of bytes read
	if (count > crud_file_table[fd].length - crud_file_table[fd].position)
	{
        // read to end of file
		int bytesRead = crud_file_table[fd].length - crud_file_table[fd].position;
		crud_file_table[fd].position = crud_file_table[fd].length;
		return bytesRead;
	}
	else
	{
		crud_file_table[fd].position += count;
    	return count;
	}
}

//////////////////////////////////////////////////////////////////////////////////////////
// Description  : Writes "count" bytes to the file handle "fh" from the
//                buffer  "buf"
//
// Inputs       : fd - the file descriptor for the file to write to
//                buf - the buffer to write
//                count - the number of bytes to write
// Outputs      : the number of bytes written or -1 if failure

int32_t crud_write(int16_t fd, void *buf, int32_t count) {
    // Check if CRUD_INIT request has been called
    if (InitFlag == 0)
    {
        // Obtain CRUD_INIT CrudRequest code and pass to crud_client_operation
        CrudRequest initialize = convert_to_CrudRequest(0, CRUD_INIT, 0, 0, 0); 
        CrudResponse initialized = crud_client_operation(initialize, NULL);
        CRParsed initParsed = parse_CrudResponse(initialized);
        // Check if CRUD_INIT was successful
        if (initParsed.res == 1)
            return -1;

        InitFlag = 1; 
    }

    // Validate parameters - good fh, buf != NULL, good length
    if (buf == NULL)
        return -1;
    if (count < 0)
        return -1;
    if (fd >= CRUD_MAX_TOTAL_FILES || fd < 0)
        return -1;
    if (crud_file_table[fd].open == 0)
        return -1;

    // Case 1 - Object does not yet exist
    if (crud_file_table[fd].object_id == 0)
    {
        // No object_id, create object
        CrudRequest create = convert_to_CrudRequest(0, CRUD_CREATE, count, 0, 0);
        CrudResponse created = crud_client_operation(create, buf);
        CRParsed newObject = parse_CrudResponse(created);
        // Check if CRUD_CREATE was successful
        if (newObject.res == 1)
            return -1;

        // Update file information
        crud_file_table[fd].object_id = newObject.oID;
        crud_file_table[fd].length = count;
        crud_file_table[fd].position = count; 

        // return number of bytes written to file
        return count; 
    }
    else // Object already exists
    {
        // Read object
        CrudRequest read = convert_to_CrudRequest(crud_file_table[fd].object_id, CRUD_READ, crud_file_table[fd].length, 0, 0);
        char *readBuf = malloc(crud_file_table[fd].length); 
        CrudResponse readResponse = crud_client_operation(read, readBuf);
        CRParsed parsedReadResponse = parse_CrudResponse(readResponse);
        // Check if CRUD_READ was successful
        if (parsedReadResponse.res == 1)
        {
            free(readBuf);
            return -1;
        }
      
        // Case 2 - writing past end of object
        if (crud_file_table[fd].position + count > crud_file_table[fd].length) 
        {
            // Allocate new buffer of appropriate size
            char *newBuf = malloc(crud_file_table[fd].position + count);
            // Copy old memory into newBuf
            memcpy(newBuf, readBuf, crud_file_table[fd].length);
            // Copy new bytes into newBuf at position 
            memcpy(&newBuf[crud_file_table[fd].position], buf, count);

            // Create new object
            CrudRequest create = convert_to_CrudRequest(0, CRUD_CREATE, crud_file_table[fd].position + count, 0, 0);
            CrudResponse created = crud_client_operation(create, newBuf);
            CRParsed newObject = parse_CrudResponse(created);
            // Free memory
            free(newBuf);
            free(readBuf);
            // Check if CRUD_CREATE was successful
            if (newObject.res == 1)
                return -1;

            // Delete old object
            CrudRequest delete = convert_to_CrudRequest(crud_file_table[fd].object_id, CRUD_DELETE, 0, 0, 0);
            CrudResponse deleted = crud_client_operation(delete, NULL);
            CRParsed deletedObject = parse_CrudResponse(deleted);
            // Check if CRUD_DELETE was successful
            if (deletedObject.res == 1)
                return -1;
            
            // Update file information
            crud_file_table[fd].object_id = newObject.oID;
            crud_file_table[fd].length = crud_file_table[fd].position + count;
            crud_file_table[fd].position += count;

            // return number of bytes written to file
            return count;
        }
        
        // Case 3 - object not changing size
        else 
        {
            // Copy bytes into buffer at position 
            memcpy(&readBuf[crud_file_table[fd].position], buf, count);

            // Update object
            CrudRequest update = convert_to_CrudRequest(crud_file_table[fd].object_id, CRUD_UPDATE, crud_file_table[fd].length, 0, 0);
            CrudResponse updated = crud_client_operation(update, readBuf);
            CRParsed updatedObject = parse_CrudResponse(updated);
            // Free memory
            free(readBuf);
            // Check if CRUD_UPDATE was successful
            if (updatedObject.res == 1)
                return -1;

            // Update file information
            crud_file_table[fd].position += count;

            // return number of bytes written to file
            return count;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_seek
// Description  : Seek to specific point in the file
//
// Inputs       : fd - the file descriptor for the file to seek
//                loc - offset from beginning of file to seek to
// Outputs      : 0 if successful or -1 if failure

int32_t crud_seek(int16_t fd, uint32_t loc) {
    // Check if CRUD_INIT request has been called
    if (InitFlag == 0)
    {
        // Obtain CRUD_INIT CrudRequest code and pass to crud_client_operation
        CrudRequest initialize = convert_to_CrudRequest(0, CRUD_INIT, 0, 0, 0); 
        CrudResponse initialized = crud_client_operation(initialize, NULL);
        CRParsed initParsed = parse_CrudResponse(initialized);
        // Check if CRUD_INIT was successful
        if (initParsed.res == 1)
            return -1;

        InitFlag = 1; 
    }

    // Validate parameters
    if (fd >= CRUD_MAX_TOTAL_FILES || fd < 0)
        return -1;
    if (crud_file_table[fd].open == 0)
        return -1;
    if (loc > crud_file_table[fd].length || loc < 0)
        return -1;

    // Set position to loc
    crud_file_table[fd].position = loc;

    return 0;
}

// Module local methods

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crudIOUnitTest
// Description  : Perform a test of the CRUD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = crud_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_client_operation(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}

































