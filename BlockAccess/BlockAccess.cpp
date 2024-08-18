#include "BlockAccess.h"

#include <cstring>

#include <iostream>


RecId BlockAccess::linearSearch(int relId,char attrName[ATTR_SIZE],union Attribute attrVal,int op){
    
    RecId prevRecId;

    RelCacheTable::getSearchIndex(relId,&prevRecId);

    RecId currRecId = {-1,-1};

    

    if(prevRecId.block == -1 && prevRecId.slot == -1){
        
        RelCatEntry relCatEntry;

        RelCacheTable::getRelCatEntry(relId,&relCatEntry);

        currRecId.block = relCatEntry.firstBlk;

        currRecId.slot = 0;

    }else{

        currRecId.block = prevRecId.block;

        currRecId.slot = prevRecId.slot + 1;

    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */

   RelCatEntry relCatBuf;

   RelCacheTable::getRelCatEntry(relId,&relCatBuf);
    
    while(currRecId.block != -1){
        // std::cout<<currRecId.block<<" "<<currRecId.slot<<std::endl;
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
        RecBuffer recBlock(currRecId.block);
        HeadInfo head;
        recBlock.getHeader(&head);

        // get the record with id (block, slot) using RecBuffer::getRecord()
        // get header of the block using RecBuffer::getHeader() function
        // get slot map of the block using RecBuffer::getSlotMap() function
        

        Attribute record[head.numAttrs]; //this is done so as to get the correct space we need..this info is accquired from the meta data of the block
        recBlock.getRecord(record,currRecId.slot);

        unsigned char slotMap[head.numSlots];
        recBlock.getSlotMap(slotMap);
        
        if(currRecId.slot >= relCatBuf.numSlotsPerBlk){
            currRecId.block = head.rblock;
            currRecId.slot = 0;
            continue;
        }
        if(slotMap[currRecId.slot] == SLOT_UNOCCUPIED){
            currRecId.slot++;
            continue;
        }


        /* use the attribute offset to get the value of the attribute from
           current record
        */

        AttrCatEntry attrCatBuf;
    
        AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatBuf);
        
        int attrOffset = attrCatBuf.offset;

        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */

        int cmpVal = compareAttrs(record[attrOffset],attrVal,attrCatBuf.attrType);


        
        if((op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0) ){
            
                 /*
                set the search index in the relation cache as
                the record id of the record that satisfies the given condition
                (use RelCacheTable::setSearchIndex function)
                */
               
                RelCacheTable::setSearchIndex(relId,&currRecId);

                return currRecId;
            }

            currRecId.slot = currRecId.slot + 1;
    }

    // no record in the relation with Id relid satisfies the given condition
    RelCacheTable::resetSearchIndex(relId);
    return RecId{-1,-1};
}
