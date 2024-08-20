#include "BlockAccess.h"

#include <cstring>

#include <iostream>

int BlockAccess::renameRelation(char oldName[ATTR_SIZE],char newName[ATTR_SIZE]){
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    char relcat_attr_relname[ATTR_SIZE] = RELCAT_ATTR_RELNAME;

    Attribute newRelationName;    // set newRelationName with newName

    strcpy(newRelationName.sVal,newName);

    // search the relation catalog for an entry with "RelName" = newRelationName

    RecId recId = linearSearch(RELCAT_RELID,relcat_attr_relname,newRelationName,EQ);

    if(recId.block != -1 && recId.slot != -1){
        return E_RELEXIST;
    }

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    Attribute oldRelationName;    // set oldRelationName with oldName

    strcpy(oldRelationName.sVal,oldName);

    // search the relation catalog for an entry with "RelName" = oldRelationName

    recId = {-1,-1};

    recId = linearSearch(RELCAT_RELID,relcat_attr_relname,oldRelationName,EQ);

    if(recId.block == -1 && recId.slot == -1){
        return E_RELNOTEXIST;
    }

    /* get the relation catalog record of the relation to rename using a RecBuffer
       on the relation catalog [RELCAT_BLOCK] and RecBuffer.getRecord function
    */
    /* update the relation name attribute in the record with newName.
       (use RELCAT_REL_NAME_INDEX) */
    // set back the record value using RecBuffer.setRecord

    Attribute rec[RELCAT_NO_ATTRS];

    RecBuffer recBuffer(RELCAT_BLOCK);

    recBuffer.getRecord(rec,recId.slot);

    strcpy(rec[RELCAT_REL_NAME_INDEX].sVal,newRelationName.sVal);

    std::cout<<rec[RELCAT_REL_NAME_INDEX].sVal<<std::endl;

    recBuffer.setRecord(rec,recId.slot);

     /*
    update all the attribute catalog entries in the attribute catalog corresponding
    to the relation with relation name oldName to the relation name newName
    */

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    int numberOfAttributes = (int) rec[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    //for i = 0 to numberOfAttributes :
    //    linearSearch on the attribute catalog for relName = oldRelationName
    //    get the record using RecBuffer.getRecord
    //
    //    update the relName field in the record to newName
    //    set back the record using RecBuffer.setRecord

    recId = {-1,-1};

    for(int idx=0;idx<numberOfAttributes;idx++){

        recId = linearSearch(ATTRCAT_RELID,relcat_attr_relname,oldRelationName,EQ);

        RecBuffer recBuffer(recId.block);

        Attribute attrRec[ATTRCAT_NO_ATTRS];

        recBuffer.getRecord(attrRec,recId.slot);

        strcpy(attrRec[ATTRCAT_REL_NAME_INDEX].sVal,newRelationName.sVal);

        std::cout<<attrRec[ATTRCAT_REL_NAME_INDEX].sVal<<std::endl;

        recBuffer.setRecord(attrRec,recId.slot);

    }

    return SUCCESS;

}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */

    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;    // set relNameAttr with newName

    strcpy(relNameAttr.sVal,relName);

    char relcat_attr_relname[ATTR_SIZE] = RELCAT_ATTR_RELNAME;

    // search the relation catalog for an entry with "RelName" = relNameAttr

    RecId recId = linearSearch(RELCAT_RELID,relcat_attr_relname,relNameAttr,EQ);

    if(recId.block == -1 && recId.slot == -1){
        return E_RELNOTEXIST;
    }

    /* reset the searchIndex of the attribute catalog using
       RelCacheTable::resetSearchIndex() */

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    /* declare variable attrToRenameRecId used to store the attr-cat recId
    of the attribute to rename */
    RecId attrToRenameRecId{-1, -1};
    recId = {-1,-1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];

    /* iterate over all Attribute Catalog Entry record corresponding to the
       relation to find the required attribute */
    
    
    
    while (true) {

        // linear search on the attribute catalog for RelName = relNameAttr
        recId = linearSearch(ATTRCAT_RELID,relcat_attr_relname,relNameAttr,EQ);
        
        if(recId.block == -1 && recId.slot == -1){
            // if there are no more attributes left to check (linearSearch returned {-1,-1})
            //     break;
            break;
        }

        RecBuffer recBuffer(recId.block);
        /* Get the record from the attribute catalog using RecBuffer.getRecord
          into attrCatEntryRecord */
        recBuffer.getRecord(attrCatEntryRecord,recId.slot);

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldName) == 0){
            // if attrCatEntryRecord.attrName = oldName
            //     attrToRenameRecId = block and slot of this record
            attrToRenameRecId.block = recId.block,attrToRenameRecId.slot = recId.slot;
        }

        if(strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName) == 0){
            // if attrCatEntryRecord.attrName = newName
            // return E_ATTREXIST;
            return E_ATTREXIST;
        }
        
    }
    if(attrToRenameRecId.block == -1 && attrToRenameRecId.slot == -1){
        // if attrToRenameRecId == {-1, -1}
        //     return E_ATTRNOTEXIST;
        return E_ATTRNOTEXIST;
    }

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    /*   declare a RecBuffer for attrToRenameRecId.block and get the record at
         attrToRenameRecId.slot */
    //   update the AttrName of the record with newName
    //   set back the record with RecBuffer.setRecord

    RecBuffer recBuffer(attrToRenameRecId.block);

    recBuffer.getRecord(attrCatEntryRecord,attrToRenameRecId.slot);

    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newName);

    recBuffer.setRecord(attrCatEntryRecord,attrToRenameRecId.slot);

    return SUCCESS;
    
    
}

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
