#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>



BlockBuffer::BlockBuffer(int blockNum){
    this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// void printDir(struct HeadInfo *head){
//     cout<<(head->blockType)<<endl;
//     cout<<(head->numEntries)<<endl;
//     cout<<(head->numAttrs)<<endl;
//     cout<<(head->numSlots)<<endl;

// }

int BlockBuffer::getHeader(struct HeadInfo *head){

    /*
        stage 2 
        unsigned char buffer[BLOCK_SIZE];
        Disk::readBlock(buffer,this->blockNum);
    */

    // details about this in the getRecord function

    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    
    if(ret != SUCCESS){
        return ret;
    }
    //copying all the bufferPtr header data to the struct headInfo

    // std::cout<<(int)bufferPtr[24]<<std::endl;

    memcpy(&head->blockType,bufferPtr,4);
    memcpy(&head->pblock,bufferPtr+4,4);
    memcpy(&head->lblock,bufferPtr+8,4);
    memcpy(&head->rblock,bufferPtr+12,4);
    memcpy(&head->numEntries,bufferPtr + 16,4);
    memcpy(&head->numAttrs,bufferPtr + 20,4);
    memcpy(&head->numSlots,bufferPtr + 24,4);
    memcpy(&head->reserved,bufferPtr + 28,4);

    return SUCCESS;
    
}

int RecBuffer::getRecord(union Attribute *rec,int slotNum){

    struct HeadInfo head;
    this->getHeader(&head); //first we get the headerInfo of the current block

    int attrCount = head.numAttrs; //from that we get the number of attributes and total numebr of slots
    int slotCount = head.numSlots;
    /*
        unsigned char buffer[BLOCK_SIZE];
        Disk::readBlock(buffer,this->blockNum);
    */
    /*
        here as said in stage 3.....the disk write and read is an async function so it will be slow to remove that we use caching and here its resolved using a 2-D array
        of size 32 x 2048  where 32 is the maximum # block possible at a time 
    */
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    int recordSize = attrCount * ATTR_SIZE;
    int offset = HEADER_SIZE + slotCount + (recordSize * slotNum);
    unsigned char *slotPointer = bufferPtr + offset; //we get the pointer by adding the offset (NOTE: slotCount and attrCount will be a constant for relation block and attribute block)

    memcpy(rec,slotPointer,recordSize);

    return SUCCESS;

}

int RecBuffer::setRecord(union Attribute *rec,int slotNum){
    
    /* 
        most of this are same as getrecord only difference is the memcpy is reversed and we need to set it to the disk 
        so we use writeBlock..
    */
    struct HeadInfo head;
    this->getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char buffer[BLOCK_SIZE];

    Disk::readBlock(buffer,this->blockNum);

    int recordSize = attrCount*ATTR_SIZE;
    int offset = HEADER_SIZE + slotCount + recordSize * slotNum;
    unsigned char *slotPointer = buffer + offset;

    //change starts here

    memcpy(slotPointer,rec,recordSize);

    Disk::writeBlock(buffer,this->blockNum);

    return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** bufferPtr){
    
    //this is done to check if the block is already in teh buffer(cache) if not we go inside the if else scope
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if(bufferNum == E_BLOCKNOTINBUFFER){

        //here we get a free buffer for the corresponding blockNum
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        //if we get an out of bound blockNum it means we got an error
        if(bufferNum == E_OUTOFBOUND){
            return E_OUTOFBOUND;
        }

        //since it is not in the buffer(cache) we load it into the cache as we can see the buffer we load is it to blocks cache
        Disk::readBlock(StaticBuffer::blocks[bufferNum],this->blockNum);

    }

    //after that we return its pointer....it points to the block in the buffer cache(ie blocks[bufferNum])
    *bufferPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/

int RecBuffer::getSlotMap(unsigned char* slotMap){
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;

    this->getHeader(&head);

    int slotCount = head.numSlots;

    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;

    memcpy(slotMap,slotMapInBuffer,slotCount);

    return SUCCESS;

}

int compareAttrs(Attribute attr1, Attribute attr2, int attrType){

    double diff;


    if(attrType == STRING){
        diff = strcmp(attr1.sVal,attr2.sVal);
    }else{
        diff = attr1.nVal - attr2.nVal;
    }

    if(diff > 0) return 1;
    else if(diff < 0) return -1;
    else return 0;
    
}
