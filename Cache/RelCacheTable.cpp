#include "RelCacheTable.h"

#include <cstring>

RelCacheEntry* RelCacheTable::relCache[MAX_OPEN];

int RelCacheTable::getRelCatEntry(int relId,RelCatEntry* relCatBuf){
    if(relId < 0 || relId>=MAX_OPEN){
        return E_OUTOFBOUND;
    }

    if(relCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }

    *relCatBuf = relCache[relId]->relCatEntry; //here we get the relcatEntry from the cache (simulation of cache memory)

    return SUCCESS;

}

/* Converts a relation catalog record to RelCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct RelCatEntry type.
NOTE: this function expects the caller to allocate memory for `*relCatEntry`
*/
void RelCacheTable::recordToRelCatEntry(union Attribute record[RELCAT_NO_ATTRS],RelCatEntry* relCatEntry){
    strcpy(relCatEntry->relName,record[RELCAT_REL_NAME_INDEX].sVal);
    relCatEntry->numAttrs = (int)record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    relCatEntry->numRecs = (int)record[RELCAT_NO_RECORDS_INDEX].nVal;
    relCatEntry->firstBlk = (int)record[RELCAT_FIRST_BLOCK_INDEX].nVal;
    relCatEntry->lastBlk = (int)record[RELCAT_LAST_BLOCK_INDEX].nVal;
    relCatEntry->numSlotsPerBlk = (int) record[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal;
}


/* will return the searchIndex for the relation corresponding to `relId
NOTE: this function expects the caller to allocate memory for `*searchIndex`
*/
int RelCacheTable::getSearchIndex(int relId,RecId* searchIndex){

    if(relId < 0 || relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }

    if(relCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }

    *searchIndex = relCache[relId]->searchIndex;

    return SUCCESS;
}

int RelCacheTable::setSearchIndex(int relId,RecId* searchIndex){

    if(relId < 0 || relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }

    if(relCache[relId] == nullptr){
        return E_RELNOTOPEN;
    }

    (relCache[relId]->searchIndex).block = searchIndex->block;
    (relCache[relId]->searchIndex).slot = searchIndex->slot;

    return SUCCESS;

}

int RelCacheTable::resetSearchIndex(int relId){

    RecId temp = {-1,-1};

    return setSearchIndex(relId,&temp);
}



