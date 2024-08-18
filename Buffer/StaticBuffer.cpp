#include "StaticBuffer.h"

#include <iostream>

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer(){

    //as this is the constructer we initialise all the buffer(cache) as unused
    for(int bufferBlockIdx = 0;bufferBlockIdx<BUFFER_CAPACITY;bufferBlockIdx++){
        metainfo[bufferBlockIdx].free = true;
    }

}
/*
    In stage 3 we wont be modifying the destructor so we just initialised the destructor
*/
StaticBuffer::~StaticBuffer(){}

int StaticBuffer::getFreeBuffer(int blockNum){

    // for checking if the blockNum overflows we return E_OUTOFBOUND
    if(blockNum < 0 || blockNum > DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    int allocatedBuffer;

    // here we check which bufferBlock is free by iterating through the metinfo of the buffer metinfo and gets the free block index
    for(int bufferBlockIdx = 0;bufferBlockIdx<BUFFER_CAPACITY;bufferBlockIdx++){
        if(metainfo[bufferBlockIdx].free){
            allocatedBuffer = bufferBlockIdx;
            break;
        }
    }

    // we make the block as taken on the metainfo 
    metainfo[allocatedBuffer].free = false;
    metainfo[allocatedBuffer].blockNum = blockNum;

    return allocatedBuffer;

}

int StaticBuffer::getBufferNum(int blockNum){

    if(blockNum < 0 || blockNum > DISK_BLOCKS){
        return E_OUTOFBOUND;
    }
    
    //this to find where we have stored the block in the cache
    for(int bufferBlockIdx = 0; bufferBlockIdx < BUFFER_CAPACITY; bufferBlockIdx++){
        if(metainfo[bufferBlockIdx].blockNum == blockNum){
            return bufferBlockIdx;
        }
    }

    return E_BLOCKNOTINBUFFER;
}
