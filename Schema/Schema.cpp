#include "Schema.h"

#include <cmath>
#include <cstring>

int Schema::openRel(char relName[ATTR_SIZE]){
    int ret = OpenRelTable::openRel(relName);
    // the OpenRelTable::openRel() function returns the rel-id if successful
    // a valid rel-id will be within the range 0 <= relId < MAX_OPEN and any
    // error codes will be negative
    if(ret >=0){
        return SUCCESS;
    }

    //otherwise it returns an error message
    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]){
    int relId = OpenRelTable::getRelId(relName);

    // this function returns the rel-id of a relation if it is open or
    // E_RELNOTOPEN if it is not. we will implement this later.
    
    if(relId < 0 || relId >= MAX_OPEN){
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}